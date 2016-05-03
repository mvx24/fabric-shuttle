import os
import subprocess
import sys

from fabric.api import env, sudo, run, hide, settings
from fabric.contrib.files import exists

# Dictionary of environments set from the fab file.
environments = {}

class SiteType:
	DJANGO = 1
	WEBAPP = 2
	NODE = 3
	SCGI = 4
	NGINX = 5

presets = {
	'GZIP_CONTENT_TYPES': ('text/html', 'text/css','application/javascript','application/x-javascript', 'image/svg+xml'),
	'NO_CACHE': 'no-cache',
	'PERMANENT_CACHE': 'max-age=315360000'
}

DJANGO_PACKAGES = ('python-pip', 'python-dev', 'sqlite3'),
DJANGO_PIP_PACKAGES = ('django', 'django-storages', 'boto', 'distribute')

# fabric.colors doesn't have bold, so just define styles here
def bold(msg):
	return '\033[1m%s\033[0m' % msg

def red(msg):
	return '\033[1;31m%s\033[0m' % msg

def green(msg):
	return '\033[1;32m%s\033[0m' % msg

def blue(msg):
	return '\033[1;34m%s\033[0m' % msg

def teal(msg):
	return '\033[1;36m%s\033[0m' % msg

def get_template_dir():
	return '%s/templates' % os.path.dirname(__file__)

def fix_static_path(path, site):
	"""Changes a relative path (os.path.abspath('.')) that may have been specified in the settings."""
	if path.startswith(os.path.abspath('.')):
		if site.get('static_inside_project', False):
			return path.replace(os.path.abspath('.'), '/srv/www/%s' % env['project'], 1)
		else:
			return path.replace(os.path.abspath('.'), '/srv/www', 1)
	return path

def fix_webapp_path(path):
	"""Like fix_static_path, but webapps should always stay inside the project."""
	if path.startswith(os.path.abspath('.')):
		return path.replace(os.path.abspath('.'), '/srv/www/%s' % env['project'], 1)
	return path

def get_webapp_taskrunner(webapp_root):
	if webapp_root.endswith('/'):
		parent = webapp_root.rsplit('/', 2)[0]
	else:
		parent = webapp_root.rsplit('/', 1)[0]
	task_runner = None
	if os.path.exists(parent + '/Gruntfile.js') or os.path.exists(parent + '/Gruntfile.coffee'):
		task_runner = 'grunt'
	elif os.path.exists(parent + '/gulpfile.js') or os.path.exists(parent + '/gulpfile.coffee'):
		task_runner = 'gulp'
	elif os.path.exists(parent + '/_config.yml'):
		task_runner = 'jekyll'
	return parent, task_runner

def build_webapp(webapp_root, task=None):
	"""Build a web app. Assumes that the webapp_root is a build subdirectory to a parent project with a build file."""
	parent, task_runner = get_webapp_taskrunner(webapp_root)
	if task_runner:
		print bold('Building %s ...' % parent)
		cwd = os.getcwd()
		os.chdir(parent)
		if not task:
			task = 'build'
		result = subprocess.call([task_runner, task])
		os.chdir(cwd)
		if result != 0:
			print red('Error: build failed!')
			raise UserWarning

_apt_get_install_set = set()

def apt_get_install(*packages):
	for package in packages:
		if package in _apt_get_install_set:
			continue
		_apt_get_install_set.add(package)
		# Check for the package
		with hide('everything'), settings(warn_only=True):
			result = run('dpkg -s %s' % package)
		if not result.succeeded:
			# TODO: separate the version from the package
			from hooks import hook
			with hook('apt-get install %s' % package):
				sudo('apt-get install %s -y' % package)
		else:
			print '%s already installed.' % package

def apt_get_update():
	sudo('apt-get update -y')

_pip_install_set = set()

def pip_install(*packages):
	for package in packages:
		if package in _pip_install_set:
			continue
		_pip_install_set.add(package)
		if package.startswith('git:') or package.startswith('git+'):
			apt_get_install('git')
		with hide('everything'), settings(warn_only=True):
			result = run('pip show %s' % package)
		# Check the output of pip show, it doesn't return non-zero on not finding the package, just no output
		if not len(result.strip()):
			# TODO: separate the version from the package
			from hooks import hook
			with hook('pip install %s' % package):
				sudo('pip install %s' % package)
		else:
			print '%s already installed.' % package

def pip_update():
	sudo('pip install --upgrade pip')
	sudo('pip install --upgrade distribute')

def set_environment(e):
	# Copy the environment name into each environments, even though only one is being used
	for name in environments:
		environments[name]['name'] = name
	# Apply the environment
	env.update(e)
	# Ensure that the sites dict exists
	if not env.get('sites'):
		env['sites'] = {}
	# Apply any default settings
	if environments.has_key('defaults'):
		for setting in environments['defaults']:
			if not env.has_key(setting):
				env[setting] = environments['defaults'][setting]
	# Apply default site settings to each site
	if env.has_key('sites') and env['sites'].has_key('defaults'):
		for site in env['sites'].values():
			for setting in env['sites']['defaults']:
				if not site.has_key(setting):
					site[setting] = env['sites']['defaults'][setting]
		del env['sites']['defaults']
	# Copy the site name into each of the sites and set the default type
	for name in env['sites']:
		env.sites[name]['name'] = name
		if not env.sites[name].has_key('type'):
			env.sites[name]['type'] = SiteType.DJANGO

def set_default_environment(e):
	for arg in sys.argv:
		if arg.startswith('e:'):
			return
	set_environment(environments[e])

def find_service(name):
	for service in env['services']:
		if service.name == name:
			return service
	return None
