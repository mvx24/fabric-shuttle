import os
import re
import StringIO
import sys

from fabric.api import task, run, sudo, put, cd, env, hide, settings, local
from fabric.contrib.files import exists

from .shared import *
from .deploy import *
from .hooks import *
import x, services

__all__ = ['services', 'environments', 'set_environment', 'set_default_environment', 'SiteType', 'presets', 'e', 's', 'vagrant', 'deploy', 'manage', 'setup', 'install', 'config', 'siteinstall', 'siteconfig', 'restart', 'stop', 'start', 'x', 'before', 'after', 'hook']

# TODO: move this to S3 usage only and use in a with statement
# Python 2.7.9 enables strict SSL cert checking, so S3 buckets with dots will no longer work
# ssl.CertificateError: hostname 'my.bucket.name.s3.amazonaws.com' doesn't match either of '*.s3.amazonaws.com', 's3.amazonaws.com'
# https://www.python.org/dev/peps/pep-0476/
import ssl
try:
	_create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
	# Legacy Python that doesn't verify HTTPS certificates by default
	pass
else:
	# Handle target environment that doesn't support HTTPS verification
	ssl._create_default_https_context = _create_unverified_https_context

# Add the project path to sys.path, so that shuttle can import from a site's settings_modules
# Will work for both a fabfile directory or file
sys.path.append(os.path.dirname(env['real_fabfile'].rstrip('/')))

class CompactStdout(object):
	def __init__(self):
		self.prefix = None
		self.in_percent = False
	def isatty(self):
		return sys.__stdout__.isatty()
	def flush(self):
		sys.__stdout__.flush()
	def write(self, s):
		# Remove all the newlines and before adding them back in, effectively removing empty lines
		if not s:
			return
		s = s.strip()
		if not s:
			self.prefix = None
			return
		if s.find('InsecureRequestWarning') != -1 or s.find('InsecurePlatformWarning') != -1 or s.find('SNIMissingWarning') != -1:
			self.prefix = None
			return
		if self.prefix:
			contains_percent = (re.search("\s*\d+%\s*", s) or re.search("[/|\\-]$", s))
			if self.in_percent:
				if contains_percent:
					sys.__stdout__.write('\r\x1b[K')
				else:
					sys.__stdout__.write('\n')
			sys.__stdout__.write(blue(self.prefix))
			self.prefix = None
			self.in_percent = contains_percent
			sys.__stdout__.write(s)
			if not self.in_percent:
				sys.__stdout__.write('\n')
		elif s.startswith('[%s]' % env['host_string']):
			self.prefix = s + ' '
			return
		elif s.lower() == 'done.':
			return
		else:
			sys.__stdout__.write(s)
			sys.__stdout__.write('\n')

sys.stdout = CompactStdout()
env['colorize_errors'] = True
env['check_production_requirements'] = True

def check_production_requirements():
	if env.get('check_production_requirements'):
		# Make sure the current branch is master, otherwise warn the user
		with hide('everything'), settings(warn_only=True):
			result = local('git status | head -1', capture=True)
		if result.succeeded and result.split()[-1] != 'master':
			answer = raw_input('Warning: Not on branch master, continue anyways? (n): ')
			if not 'yes'.startswith(answer.lower()):
				exit(0)

@task
def f():
	""" Force the operation by ignoring any requirements and restrictions. """
	env['check_production_requirements'] = False

@task
def e(name):
	""" Set the environment for use with other commands. """
	if name:
		name = name.lower()
	if environments.has_key(name):
		set_environment(environments[name])
	else:
		print "Error: Unknown environment specified."

@task
def s(name):
	""" Set the site to use with other commands. Required: siteinstall, siteconfig, manage, and x:deploywebapp, Optional: setup, deploy. Ignored by all other tasks.  """
	if name:
		name = name.lower()
	if not env.has_key('sites'):
		print red('Error: An environment with sites must be specified before a site.')
		exit(1)
	if not env['sites'].has_key(name):
		print red('Error: Unknown site specified.')
		exit(1)
	env['site'] = env['sites'][name]

# Supported vars: http://docs.fabfile.org/en/1.4.0/usage/execution.html#ssh-config
__SSH_CONFIG_MAP = { 'User': 'user', 'Port': 'port', 'HostName': 'hosts', 'IdentityFile': 'key_filename', 'ForwardAgent': 'forward_agent' }

@task
def vagrant():
	""" Use to override the fab environment with information taken from vagrant ssh_config. """
	# If no other environment is set then use an environment named vagrant
	if not env.get('default') and environments.has_key('vagrant'):
		set_default_environment('vagrant')
	# Force the operation by treating vagrant as always a non-production host
	f()
	env['vagrant'] = True
	env['use_ssh_config'] = False
	env['disable_known_hosts'] = True
	with hook('vagrant'):
		with hide('everything'), settings(warn_only=True):
			result = local('vagrant ssh-config', capture=True)
		if result.failed:
			print red(result)
			exit(1)
		results = result.splitlines()
		for line in results:
			key, value = line.strip().split()
			if key in __SSH_CONFIG_MAP:
				env[__SSH_CONFIG_MAP[key]] = value.strip('"')
		if type(env['hosts']) is str:
			env['hosts'] = [env['hosts']]

@task
def deploy():
	""" Deploy the project to the server using rsync, if site is specified, then will deploy a webapp. """
	check_production_requirements()
	site = env.get('site')
	with hook('deploy'):
		# Handle webapp deployments
		deploy_webapp()
		# Handle Django deployments
		sites = env['sites'].values() if site is None else [site]
		sites = [site for site in sites if site['type'] == SiteType.DJANGO]
		if sites:
			for site in sites:
				# Run local tests
				# TODO: support local virtualenv for testing
				if site.has_key('local_tests') and site['local_tests']:
					local('python manage.py test %s --settings %s' % (' '.join(site['local_tests']), site['settings_module']))
			uwsgi = find_service('uwsgi')
			nginx = find_service('nginx')
			supervisor = find_service('supervisor')
			uwsgi.stop()
			nginx.stop()
			django_sync(sites)
			with own_project():
				with cd(get_project_directory()):
					for site in sites:
						django_append_settings(site)
						python = get_python_interpreter(site)
						# Migrate the database
						# Only syncdb for versions below 1.7
						#sudo('python manage.py syncdb --settings %s --noinput' % site['settings_module'])
						sudo('%s manage.py migrate --settings %s --noinput' % (python, site['settings_module']))
						# Collect static files
						# The collectstatic with --clear with raise an exception and fail if the static directory does not already exist, so only clear if it exists
						clear = '--clear' if exists(get_static_root(site)) else ''
						sudo('%s manage.py collectstatic --settings %s --noinput %s' % (python, site['settings_module'], clear))
						chown(get_static_root(site), WWW_USER, WWW_USER)
						# Apply fixtures
						fixtures = site.get('fixtures', [])
						if fixtures:
							sudo('%s manage.py loaddata --settings %s %s' % (python, site['settings_module'], ' '.join(fixtures)))
						# Run remote tests
						if site.has_key('remote_tests') and site['remote_tests']:
							with settings(warn_only=True):
								sudo('%s manage.py test %s --settings %s ' % (python, ' '.join(site['remote_tests']), site['settings_module']))
						# Enable the site for nginx and uwsgi
						with hide('warnings'), settings(warn_only=True):
							sudo('ln -sf /etc/nginx/sites-available/%s.conf /etc/nginx/sites-enabled/%s.conf' % (site['name'], site['name']))
							sudo('ln -sf /etc/uwsgi/apps-available/%s.ini /etc/uwsgi/apps-enabled/%s.ini' % (site['name'], site['name']))
			uwsgi.start()
			nginx.start()
			if supervisor:
				supervisor.restart()

@task
def manage(*args):
	""" Run a management command using the supplied arguments. If site is not specified command will be run on all sites. """
	site = env.get('site')
	project_dir = get_project_directory()
	with hook('manage %s' % args[0], args[1:]):
		if site is not None:
			python = get_python_interpreter(site)
			with cd(project_dir):
				sudo('%s manage.py %s --settings %s' % (python, ' '.join(args), site['settings_module']), user=WWW_USER)
		else:
			for site in env['sites'].values():
				python = get_python_interpreter(site)
				with cd(project_dir):
					sudo('%s manage.py %s --settings %s' % (python, ' '.join(args), site['settings_module']), user=WWW_USER)

# Service tasks

@task
def setup(*service_names):
	""" Setup a server from scratch by installing and configuring services for the server and the sites. """
	env['setup'] = True
	check_production_requirements()
	install(*service_names)
	config(*service_names)
	siteinstall(*service_names)
	siteconfig(*service_names)

@task
def install(*service_names):
	""" Install services on the server. """
	# Install the minimum common packages first
	apt_get_update()
	apt_get_install('gcc', 'make', 'linux-headers-$(uname -r)', 'build-essential', 'libtool', 'autoconf', 'libssl-dev', 'libffi-dev', 'zip')
	for service in env['services']:
		if not service_names or service.name in service_names:
			service.install()

@task
def config(*service_names):
	""" Configure services on the server. """
	check_production_requirements()
	for service in env['services']:
		if not service_names or service.name in service_names:
			service.config()

@task
def siteinstall(*service_names):
	""" Install packages needed for services for one or all sites. """
	site = env.get('site')
	sites = env['sites'].values() if site is None else [site]
	# Run initial setup for each type of site
	site_types = set([s['type'] for s in sites])
	for t in site_types:
		if t == SiteType.DJANGO:
			apt_get_install('python-pip', 'python-dev', 'sqlite3')
			pip_install(None, 'virtualenv')
	# Install packages and each service for each site
	for s in sites:
		print teal(s['name'] + ' siteinstall')
		if s.get('packages'):
			apt_get_install(*s['packages'])
		pip_install(s)
		for service in s.get('services', []):
			if not service_names or service.name in service_names:
				service.site_install(s)

@task
def siteconfig(*service_names):
	""" Configure the services for one or all sites. """
	check_production_requirements()
	site = env.get('site')
	sites = env['sites'].values() if site is None else [site]
	for s in sites:
		print teal(s['name'] + ' siteconfig')
		for service in s.get('services', []):
			if not service_names or service.name in service_names:
				service.site_config(s)

@task
def restart(*service_names):
	""" Restart one or more services. """
	for name in service_names:
		service = find_service(name)
		if service:
			service.restart()
		else:
			print red('No such service: %s' % name)

@task
def start(*service_names):
	""" Start one or more services. """
	for name in service_names:
		service = find_service(name)
		if service:
			service.start()
		else:
			print red('No such service: %s' % name)

@task
def stop(*service_names):
	""" Stop one or more services. """
	for name in service_names:
		service = find_service(name)
		if service:
			service.stop()
		else:
			print red('No such service: %s' % name)

# x commands

X_COMMAND_MAP = {
	'e': e,
	's': s,
	'installkey': x.install_key,
	'localsetup': x.local_setup,
	'generates3ajax': x.generate_s3_ajax,
	'deploywebapp': deploy_webapp,
	'dryrun': django_sync_dry_run,
	'uname': x.uname,
	'ntp': x.ntp,
	'exports': x.exports,
	'archive': x.archive,
	'reboot': x.reboot,
	'runwebapp': x.run_webapp
}

@task
def x(command=None, *args):
	""" Run another internal command meant for development purposes. For a full list run: fab x """
	if not command:
		print 'Available x commands:\n'
		justification = len(max(X_COMMAND_MAP.keys(), key=len)) + 1
		for name in sorted(X_COMMAND_MAP.keys()):
			print '    %s%s' % (name.ljust(justification), X_COMMAND_MAP[name].__doc__)
	else:
		fun = X_COMMAND_MAP.get(command, None)
		if fun:
			fun(*args)
		else:
			print red('\nError: no such command.\n')
			x()
