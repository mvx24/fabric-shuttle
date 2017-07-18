from importlib import import_module
import os
import subprocess
import sys
import time

from fabric.api import env, sudo, run, hide, settings, local
from fabric.contrib.files import exists

WWW_USER = 'www-data'

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

def split_package(package):
	"""Split a package into the name and version components."""
	name = package
	version = ''
	for c in ('!', '<', '>', '=', '~', '^'):
		if package.find(c) != -1:
			n, v = package.split(c, 1)
			n = n.strip()
			v = c + v.strip()
			if len(v) > len(version):
				name = n
				version = v
	return name, version

class cd_local(object):
	"""Changes the local current working directory."""
	def __init__(self, nwd):
		self.cwd = os.getcwd()
		self.nwd = nwd
	def __enter__(self):
		os.chdir(self.nwd)
	def __exit__(self, *_):
		os.chdir(self.cwd)

def get_template_directory():
	return '%s/templates' % os.path.dirname(__file__)

def get_template(name):
	return os.path.join(get_template_directory(), name)

def chown(paths, username='root', group='root'):
	"""Change the owner and group of a path or multiple paths. The return value from put() or upload_template() can be wrapped in this directly."""
	if not paths:
		return
	if isinstance(paths, (str, unicode)):
		paths = [paths]
	for path in paths:
		sudo('chown -R %s:%s %s' % (username, group, path))

def compare_files(local_path, remote_path):
	with hide('everything'), settings(warn_only=True):
		local_result = local('cksum ' + local_path, capture=True)
		remote_result = sudo('cksum ' + remote_path)
		if local_result.succeeded and remote_result.succeeded:
			# cksum output is: <sum> <bytes> <path> e.g. 944758468 222 requirements.txt
			# compare only the first two because path could be different
			return local_result[:local_result.rfind(' ')] == remote_result[:remote_result.rfind(' ')]
	return False

def find_static(site, name):
	"""Finds a static file in a Django project with appname/static and STATICFILES_DIRS without resorting to importing the Django finder."""
	static_dirs = get_django_setting(site, 'STATICFILES_DIRS')
	if static_dirs:
		for path in static_dirs:
			if isinstance(path, (list, tuple)):
				path = path[1]
			path = os.path.join(path, name)
			if os.path.exists(path):
				return path
	apps = get_django_setting(site, 'INSTALLED_APPS')
	import imp
	for app in apps:
		app = app.split('.', 1)
		try:
			path = imp.find_module(app[0])[1]
		except:
			continue
		if len(app) > 1:
			path = os.path.join(path, app[1].replace('.', '/'))
		path = os.path.join(path, 'static', name)
		if os.path.exists(path):
			return path
	return None

def fix_absolute_path(path):
	"""If settings spill over from development for a path inside the project then translate it to the project directory on the server."""
	if path.startswith(os.path.abspath('.')):
		return path.replace(os.path.abspath('.'), get_project_directory(), 1)
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

_build_webapp_set = set()

def build_webapp(webapp_root, task=None):
	"""Build a web app. Assumes that the webapp_root is a build subdirectory to a parent project with a build file."""
	parent, task_runner = get_webapp_taskrunner(webapp_root)
	if not task_runner:
		return
	if not task:
		task = 'build'
	build_cmd = ' '.join((parent, task_runner, task))
	if build_cmd in _build_webapp_set:
		return
	_build_webapp_set.add(build_cmd)
	def webapp_cmd(cmd):
		result = local(cmd)
		if result.failed:
			print red('Error running %s!' % cmd)
			raise UserWarning
	with cd_local(parent):
		# Install or update front-end packages
		if os.path.exists(os.path.join(parent, 'package.json')):
			if os.path.exists(os.path.join(parent, 'node_modules')):
				print bold('Updating npm packages ...')
				webapp_cmd('npm update')
			else:
				print bold('Installing npm packages ...')
				webapp_cmd('npm install')
		if os.path.exists(os.path.join(parent, 'bower.json')):
			if os.path.exists(os.path.join(parent, 'bower_components')):
				print bold('Updating bower packages ...')
				webapp_cmd('bower update')
			else:
				print bold('Installing bower packages ...')
				webapp_cmd('bower install')
		if os.path.exists(os.path.join(parent, 'config.js')):
			if os.path.exists(os.path.join(parent, 'jspm_packages')):
				print bold('Updating jspm packages ...')
				webapp_cmd('jspm update')
			else:
				print bold('Installing jspm packages ...')
				webapp_cmd('jspm install')
		# Run the build task
		print bold('Building %s ...' % parent)
		webapp_cmd('%s %s' % (task_runner, task))

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
	# Don't update apt-get if not older than a day
	with hide('everything'), settings(warn_only=True):
		result = run('stat -c %Z /var/cache/apt/pkgcache.bin')
		if not env.get('setup') and result.succeeded and (int(time.time()) - int(result)) < (24 * 60 * 60):
			print 'apt-get is up to date'
			return
	sudo('apt-get update -y')

def apt_get_upgrade_packages():
	sudo('apt-get upgrade -y')

def get_project_directory():
	return os.path.join('/srv/www/apps', env['project'])

def get_manage_directory():
	return os.path.join('/srv/www/manage', env['project'])

def get_requirements_packages():
	try:
		with open('requirements.txt') as f:
			# TODO: remove blank lines
			return tuple(map(lambda package: package.strip(), f.readlines()))
	except:
		return tuple()

def get_django_setting(site, setting):
	try:
		module = import_module(site['settings_module'])
		return getattr(module, setting)
	except:
		return None

def get_static_root(site):
	"""Get the STATIC_ROOT for a Django site. If not set and STATIC_ROOT is the default None then set to a production default."""
	static_root = get_django_setting(site, 'STATIC_ROOT')
	if not static_root:
		# Nevermind, static needs to be set in Django or it seems like it will throw an error
		return os.path.join('/srv/www/static', site['name'])
	return fix_absolute_path(static_root)

def get_media_root(site):
	"""Get the MEDIA_ROOT for a Django site. If not set and MEDIA_ROOT is the default '' then set to a production default."""
	media_root = get_django_setting(site, 'MEDIA_ROOT')
	if not media_root:
		return os.path.join('/srv/www/media', site['name'])
	return fix_absolute_path(media_root)

def get_webapp_root(site):
	"""Get the WEBAPP_ROOT but do not provide a production default if not set."""
	webapp_root = get_django_setting(site, 'WEBAPP_ROOT')
	if webapp_root:
		webapp_root = fix_absolute_path(webapp_root)
	return webapp_root

def get_static_url(site):
	"""Get the STATIC_URL for a Django site. If not set and STATIC_URL is the default None then set to a production default."""
	return get_django_setting(site, 'STATIC_URL') or '/static/'

def get_media_url(site):
	"""Get the MEDIA_URL for a Django site. If not set and MEDIA_URL is the default '' then set to a production default."""
	return get_django_setting(site, 'MEDIA_URL') or '/media/'

def get_webapp_url(site):
	return get_django_setting(site, 'WEBAPP_URL') or '/'

def get_virtual_env(site=None):
	# If virtualenv is available, then setup and use it
	with hide('everything'), settings(warn_only=True):
		if site and run('which virtualenv').succeeded:
			virtual_env = os.path.join('/srv/www/env', site['name'])
			if not exists(virtual_env):
				sudo('virtualenv ' + virtual_env)
			return virtual_env

def get_python_interpreter(site=None):
	virtual_env = get_virtual_env(site)
	if virtual_env:
		return os.path.join(virtual_env, 'bin/python')
	else:
		return 'python'

def get_pip_installer(site=None):
	virtual_env = get_virtual_env(site)
	if virtual_env:
		return os.path.join(virtual_env, 'bin/pip')
	else:
		with hide('everything'), settings(warn_only=True):
			if run('which pip', True).succeeded:
				return 'pip'
			else:
				return 'easy_install'

_pip_install_set = set()

def pip_install(site=None, *packages):
	# TODO: support upgrading packages with pip install --upgrade packagename
	# Check for pip first
	with hide('everything'), settings(warn_only=True):
		result = run('dpkg -s python-pip')
	if not result.succeeded:
		apt_get_install('python-pip')
	if site and len(packages) == 0:
		packages = tuple(site.get('pip_packages', [])) + get_requirements_packages()
	pip = get_pip_installer(site)
	# Install each listed package
	for package in packages:
		name, version = split_package(package)
		if site is None:
			if package in _pip_install_set:
				continue
			_pip_install_set.add(package)
		if package.startswith('git:') or package.startswith('git+'):
			apt_get_install('git')
		from hooks import hook
		with hook('pip install %s' % name):
			# sudo with -H for setting the home directory for root so pip has proper permissions to cache
			run('sudo -H %s install "%s"' % (pip, package))

def pip_check_install(package, site=None):
	pip = get_pip_installer(site)
	name, version = split_package(package)
	with hide('everything'), settings(warn_only=True):
		result = run('%s show "%s" 2>/dev/null' % (pip, name))
	# Check the output of pip show, it might not return non-zero on not finding the package, just no output
	return not (result.failed or not len(result.strip()))

def pip_update():
	# WARNING: Upgrading pip to the latest version will cause urllib3 warnings and errors to start occuring on Ubuntu 14
	# virtualenv uses the latest versions and will cause these warnings, but they are consumed in CompactStdout
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
		env['sites'][name]['name'] = name
		if not env['sites'][name].has_key('type'):
			env['sites'][name]['type'] = SiteType.DJANGO

def set_default_environment(e):
	for arg in sys.argv:
		if arg.startswith('e:'):
			return
	set_environment(environments[e])
	env['default'] = True

def find_service(name):
	for service in env['services']:
		if service.name == name:
			return service
	return None
