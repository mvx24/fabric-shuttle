from importlib import import_module
import os

from fabric.api import put, run, sudo, settings, env, local

from .hooks import hook
from .services.s3 import S3, get_aws_access_key
from .shared import *

def reboot():
	"""Reboot the server."""
	with hook('reboot'):
		sudo('shutdown -r now')

def install_key(key=None):
	"""Copy an ssh key to login later."""
	with settings(warn_only=True):
		run('mkdir -p .ssh')
		if not key:
			put('~/.ssh/id_rsa.pub', '.ssh/authorized_keys')
		else:
			put('~/.ssh/%s' % key, '.ssh/authorized_keys')

def generate_s3_ajax(module_name=''):
	"""Generate jquery ajax code for saving files with the S3 policy documents. A site must be specified."""
	# http://aws.amazon.com/articles/1434
	# http://docs.aws.amazon.com/AmazonS3/2006-03-01/dev/HTTPPOSTForms.html
	site = env.get('site')
	if not site:
		print red('Error: An environment and site must both first be specified.')
		exit(1)
	for s3_service in site['services']:
		if type(s3_service) is S3:
			break
		s3_service = None
	if not s3_service:
		print red('Error: There is no S3 service associated with this site.')
		exit(1)
	site_access_key_id, site_secret_access_key = get_aws_access_key(site)
	from django.conf import settings
	from django.template import Template, Context
	import json, hmac, hashlib, base64
	settings.configure()
	with open('%s/templates/s3-ajax.js' % os.path.dirname(__file__)) as f:
		template = Template(f.read())
	context = {'policies': [], 'module_name': module_name}
	policy_context_vars = ('success_action_redirect', 'bucket', 'acl')
	for name, policy in s3_service.settings['policy_documents'].items():
		aws_access_key_id = policy.pop('access_key_id', site_access_key_id)
		aws_secret_access_key = policy.pop('secret_access_key', site_secret_access_key)
		policy_str = json.dumps(policy)
		policy['access_key_id'] = aws_access_key_id
		policy['headers'] = {}
		policy['meta_data'] = []
		# Process the conditions
		for condition in policy['conditions']:
			if type(condition) is dict:
				key = condition.keys()[0]
				value = condition.values()[0]
			elif type(condition) in (list, tuple):
				# The first value will always be starts-with or eq but they are treated the same
				if condition[0] not in ('starts-with', 'eq'):
					continue
				key = condition[1][1:]
				value = condition[2]
				if key == 'key':
					policy['directory'] = value
					continue
			if key in policy_context_vars:
				policy[key] = value
			elif key == 'Expires' or key.startswith('Content-'):
				policy['headers'][key] = value
			elif key.startswith('x-amz-meta-'):
				policy['meta_data'].append(key[11:])
		policy['name'] = name
		policy['encoded'] = base64.b64encode(policy_str)
		policy['signature'] = base64.b64encode(hmac.new(aws_secret_access_key, policy['encoded'], hashlib.sha1).digest())
		context['policies'].append(policy)
	print template.render(Context(context, autoescape=False))

_webapp_process = None

def _signal_handler(sig, frame):
	import signal, sys
	signal.signal(signal.SIGINT, signal.SIG_IGN)
	if _webapp_process:
		_webapp_process.kill()
	sys.exit(0)

def run_webapp():
	"""Run a webapp. Serve up a stand-alone webapp site on port 8000. Assumes that the webapp_root is a build subdirectory to a parent project with a build file."""
	site = env.get('site')
	if not site:
		print red('Error: An environment and site must both first be specified.')
		exit(1)
	if not site.has_key('webapp') or not site['webapp'].get('root'):
		print red('Error: Site must be a stand-alone webapp with a root specified.')
		exit(1)
	import BaseHTTPServer, SimpleHTTPServer
	parent, task_runner = get_webapp_taskrunner(site['webapp']['root'])
	if task_runner == 'jekyll':
		build_webapp(site['webapp']['root'], site['webapp'].get('build_task'))
	elif task_runner:
		import subprocess, signal
		signal.signal(signal.SIGINT, _signal_handler)
		global _webapp_process
		print bold('Running %s ...' % parent)
		cwd = os.getcwd()
		os.chdir(parent)
		task = site['webapp'].get('run_task', 'runwebapp')
		_webapp_process = subprocess.Popen([task_runner, task])
		os.chdir(cwd)
	class WebAppRequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler, object):
		def do_GET(self):
			# Strip any query arguments
			self.path = self.path.split('?')[0]
			# Convert to an index.html file
			if self.path.endswith('/'):
				self.path = self.path + 'index.html'
			# Try to rewrite as one of the extra webapp files
			files = site['webapp'].get('files')
			if files:
				dirname, filename = os.path.split(self.path)
				dirname = dirname.strip('/')
				if type(files) is dict:
					for prefix in files:
						if prefix.strip('/') == dirname:
							files = files[prefix]
							break
				if isinstance(files, (list, tuple)):
					for f in files:
						if filename == os.path.basename(f):
							self.path = '/%s' % f
							return super(WebAppRequestHandler, self).do_GET()
			# Rewrite to the webapp root
			self.path = '/' + site['webapp']['root'].strip('/') + self.path
			return super(WebAppRequestHandler, self).do_GET()
	httpd = BaseHTTPServer.HTTPServer(('', 8000), WebAppRequestHandler)
	print "Serving on port %d" % 8000
	httpd.serve_forever()

def uname():
	"""Print the remote uname."""
	run('uname -a')

def ntp():
	"""Sync the time on the host using network time."""
	sudo('ntpdate pool.ntp.org')

def exports():
	"""Prints commands to export AWS environment variables. Example use: eval `fab --hide running,status e:production x:exports`"""
	site = env.get('site')
	if not site:
		print red('Error: An environment and site must both first be specified.')
		exit(1)
	else:
		aws_access_key_id, aws_secret_access_key = get_aws_access_key(site)
		# Command line interface variables
		print 'export AWS_ACCESS_KEY=%s' % aws_access_key_id
		print 'export AWS_SECRET_KEY=%s' % aws_secret_access_key
		# Boto variables
		print 'export AWS_ACCESS_KEY_ID=%s' % aws_access_key_id
		print 'export AWS_SECRET_ACCESS_KEY=%s' % aws_secret_access_key

# Archive functions

def __git_archive():
	"""Creates a git archive with all submodules included. Uses gnutar so the archive is compatible on linux."""
	project = env.get('project', 'project')
	local("git archive HEAD -o %s.tar; git submodule foreach 'git archive --prefix ${path}/ HEAD -o ../temp.tar; gnutar -Af ../%s.tar ../temp.tar; rm ../temp.tar'; gzip -f %s.tar" % (project, project, project))

def __git_raw_archive():
	"""Creates an archive of files not based on the git repository. For testing uncommitted code."""
	project = env.get('project', 'project')
	local("gnutar -cf %s.tar .; gzip -f %s.tar" % (project, project))

def archive(raw=False):
	"""Creates a git archive with all submodules included. Uses gnutar so the archive is compatible on linux. Optional argument: true for a raw archive."""
	if raw:
		__git_raw_archive()
	else:
		__git_archive()

_MYSQL_CLIENT_INSTRUCTIONS = """1. Download the connector from: http://dev.mysql.com/downloads/connector/c/
2. Install the connector into /usr/local/
3. Add the correct /usr/local/mysql-connector-c-<your version here>/bin directory to PATH
4. Add the correct /usr/local/mysql-connector-c-<your version here>/lib directory to DYLD_LIBRARY_PATH"""

_MYSQL_PYTHON_INSTRUCTIONS = """1. Download MySQL-python from: https://pypi.python.org/pypi/MySQL-python
2. export CFLAGS=-Wunused-command-line-argument-hard-error-in-future
3. python setup.py build
4. sudo python setup.py install
"""

_PYCRYPTO_INSTRUCTIONS = """1. Download pycrypto from: https://www.dlitz.net/software/pycrypto/
2. export CFLAGS=-Wunused-command-line-argument-hard-error-in-future
3. ./configure
4. python setup.py build
5. sudo python setup.py install
"""

def local_setup():
	"""Installs packages necessary for a local mac setup."""
	with settings(warn_only=True):
		packages = tuple(env.get('pip_packages', [])) + get_requirements_packages()
		# Check to make sure mysql client libraries are installed
		if 'mysql-python' in packages:
			result = local('which mysql_config', True)
			if not result.succeeded:
				# Print mysql client setup instructions and exit
				print red('You do not have the mysql client libraries installed.')
				print 'Please follow these instructions first:'
				print _MYSQL_CLIENT_INSTRUCTIONS
				return

			try:
				import MySQLdb
			except:
				import platform
				if platform.system() == 'Darwin':
					print red('You do not have MySQL-python installed.')
					print 'Please install first from easy_install, pip, or the instructions below:'
					print _MYSQL_PYTHON_INSTRUCTIONS
					return

		try:
			import Crypto
		except:
			import platform
			if platform.system() == 'Darwin':
				print red('You do not have pycrypto installed.')
				print 'Please install first from easy_install, pip, or the instructions below:'
				print _PYCRYPTO_INSTRUCTIONS
				return

		print 'The following packages will be installed:'
		print '\n'.join(packages)
		print 'Continue? y/n:'
		i = None
		while not i: i = raw_input().lower()
		if i[0] != 'y':
			return
		# Activate the virtualenv if setup
		pip = os.path.join(os.path.abspath('.'), 'env/bin/pip')
		if not os.path.isfile(pip):
			pip = None
			print 'Could not detect a virtualenv:'
			print 'Continue? y/n:'
			i = None
			while not i: i = raw_input().lower()
			if i[0] != 'y':
				return
		if pip or local('which pip', True).succeeded:
			pip = pip or 'pip'
			for package in packages:
				local('sudo %s install %s' % (pip, package))
				if package == 'distribute':
					local('sudo %s install --upgrade distribute' % pip)
		else:
			for package in packages:
				if package.startswith('git+git'):
					package = package.replace('git+git', 'https', 1)
					package = package.replace('.git#', '/tarball/master#', 1)
				local('sudo easy_install %s' % package)
				if package == 'distribute':
					local('sudo easy_install -U distribute')
		print ''
		print bold('Note: if you have webapps with this project, these need to be setup seperately - potentially requiring nodejs and running npm install.')
