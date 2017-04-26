from importlib import import_module
import os
import subprocess

from fabric.api import sudo, local, put, settings
from fabric.contrib.files import append, upload_template

from .services.s3 import upload_to_s3, delete_from_s3
from .shared import *

class own_project(object):
	"""Changes the permissions of the project between the ssh user and nginx user."""
	def __init__(self):
		pass
	def __enter__(self):
		chown(get_project_directory(), env['user'], env['user'])
	def __exit__(self, *_):
		chown(get_project_directory(), WWW_USER, WWW_USER)

def _get_remote_shell():
	parts = ['ssh', '-p', env.get('port', '22')]
	if env.get('key_filename'):
		parts.append('-i')
		parts.append(os.path.expanduser(env['key_filename']))
	if env.get('vagrant'):
		parts.append('-o')
		parts.append('UserKnownHostsFile=/dev/null')
		parts.append('-o')
		parts.append('StrictHostKeyChecking=no')
	return ' '.join(parts)

def _django_get_excluded(sites):
	"""Get an excluded list of patterns.

	rsync patterns are similar to .gitignore patterns. e.g. /foo matches foo at the base.
	http://git-scm.com/docs/gitignore
	http://ss64.com/bash/rsync.html
	"""
	excluded = ['.*', '*.pyc', '*.db', 'Vagrantfile']
	# Exclude anything additionally specified in the environment
	if env.has_key('excluded_files') and env['excluded_files']:
		excluded.extend(env['excluded_files'])
	# Exclude anything in the root .gitignore file
	if os.path.exists('.gitignore'):
		with file('.gitignore', 'r') as f:
			for line in f:
				line = line.strip()
				if line.startswith('.') or line.find('!') != -1:
					continue
				if line not in excluded:
					excluded.append(line)
	# Exlude all of the webapp's node_modules and bower_components
	for site in sites:
		webapp_root = get_django_setting(site, 'WEBAPP_ROOT')
		if webapp_root:
			try:
				parent, task_runner = get_webapp_taskrunner(webapp_root)
				if task_runner:
					excluded.append('/' + os.path.relpath(parent) + '/node_modules')
					excluded.append('/' + os.path.relpath(parent) + '/bower_components')
				# If wanting to exclude the entire webapp
				#if task_runner:
				#	excluded.append('/' + os.path.relpath(parent))
				#else:
				#	excluded.append('/' + os.path.relpath(webapp_root))
			except:
				print red('Warning: Could not import webapp settings for %s when syncing.' % site['name'])
	return excluded

SYNC_COMMAND = 'rsync -avz %s --delete --delete-excluded%s ./ %s@%s:%s'

def django_sync(sites):
	"""Syncs the local code to the server. Used as part of the deploy process. Wrap in the task decorator to enable. sync = task(sync)"""
	project_dir = project_subpath = get_project_directory()
	if not project_subpath.endswith('/'):
		project_subpath += '/'
	sudo('mkdir -p %s' % project_dir)
	manage_dir = get_manage_directory()
	sudo('mkdir -p %s' % manage_dir)
	with own_project():
		# Preserve existing media if a subdirectory of the project
		for site in sites:
			media_root = get_media_root(site)
			if media_root.startswith(project_subpath) and exists(media_root):
				sudo('mv %s /tmp/%smedia' % (media_root, env['project']))

		# Sync and set the owner
		excluded = _django_get_excluded(sites)
		excluded = ['--exclude="%s"' % ex for ex in excluded]
		local(SYNC_COMMAND % (' '.join(excluded), ' -e "%s"' % _get_remote_shell(), env['user'], env['hosts'][0], project_dir))

		# Create the media directory
		# Restore existing media if a subdirectory of the project, if it is outside the project make sure it is owned by nginx
		for site in sites:
			media_root = get_media_root(site)
			sudo('mkdir -p %s' % media_root)
			if media_root.startswith(project_subpath):
				if exists('/tmp/%smedia' % env['project']):
					# Delete the media directory but leave intermediate directories then restore with a move
					sudo('rm -rf %s' % media_root)
					sudo('mv /tmp/%smedia %s' % (env['project'], media_root))
			else:
				chown(media_root, WWW_USER, WWW_USER)

			# Copy any additional webapp files
			if site.has_key('webapp') and site['webapp'].get('files'):
				webapp_root = get_webapp_root(site)
				if webapp_root:
					for filename in site['webapp']['files']:
						result = find_static(site, filename)
						if not result:
							print red('Error: Could not find static file %s.' % filename)
							return
						chown(put(result, os.path.join(webapp_root, filename), use_sudo=True, mode=0644), WWW_USER, WWW_USER)

		for site in sites:
			# Create manage.py shortcuts the manage directory
			context = {'project_dir': project_dir, 'settings_module': site['settings_module'], 'interpreter': get_python_interpreter(site)}
			chown(upload_template(get_template('manage.py'), os.path.join(manage_dir, site['name'] + '.py'), context=context, backup=False, use_sudo=True, mode=0755), WWW_USER, WWW_USER)

def django_sync_dry_run(sites):
	"""Do an rsync dry run to see which files will be updated when deploying."""
	# e.g. To show just migrations: fab e:production x:dryrun | grep -v "^deleting" | grep -v "/$" | grep "^shared/migrations"
	excluded = _django_get_excluded(sites)
	excluded = ['--exclude="%s"' % ex for ex in excluded]
	local(SYNC_COMMAND % (' '.join(excluded), ' --dry-run -e "%s"' % _get_remote_shell(), env['user'], env['hosts'][0], get_project_directory()))

def django_sync_down(path=''):
	"""Syncs down stuff from the server. Wrap in the task decorator to enable. sync_down = task(sync_down)"""
	if path.startswith('/'):
		path = path[1:]
	if not path.endswith('/'):
		path += '/'
	remote_path = os.path.join(get_project_directory(), path)
	local('rsync -avz --exclude=".*" --exclude="*.pyc" --exclude="*.sh" --exclude="*.db" -e "%s" %s@%s:%s ./%s' % (_get_remote_shell(), env['user'], env['hosts'][0], remote_path, path))

def django_append_settings(site):
	"""Update Django settings with production ready values, possibly not set explicitly in the settings."""
	filename = fix_absolute_path(os.path.abspath(site['settings_module'].replace('.', '/'))) + '.py'
	txt = """
	####################################
	# Default/Fixed Paths from Shuttle #

	MEDIA_ROOT = '%s'
	MEDIA_URL = '%s'
	STATIC_ROOT = '%s'
	STATIC_URL = '%s'
	""" % (get_media_root(site), get_media_url(site), get_static_root(site), get_static_url(site))
	webapp_root = get_webapp_root(site)
	if webapp_root:
		txt += "\nWEBAPP_ROOT = '%s'\nWEBAPP_URL = '%s'\n" % (webapp_root, get_webapp_url(site))
	# For vagrant dev boxes add localhost to the ALLOWED_HOSTS
	if env.get('vagrant'):
		allowed_hosts = ', '.join(["'%s'" % host for host in (get_django_setting(site, 'ALLOWED_HOSTS') or [site['name']])])
		txt += "\nALLOWED_HOSTS = [%s, 'localhost']\n" % allowed_hosts
	# Each service might have additional settings
	for service in site.get('services', []):
		site_settings = service.get_site_settings(site)
		if site_settings:
			txt += '\n' + '\n'.join(["%s = '%s'" % item for item in site_settings.items()]) + '\n'
	append(filename, txt.replace('\t', ''), use_sudo=True)

def deploy_webapp():
	"""Deploy a webapp to S3 with the prefix of WEBAPP_URL. If site is not specified, then the command will be run on all sites."""
	site = env.get('site')
	if not site:
		for site in env['sites'].values():
			env['site'] = site
			deploy_webapp()
		del env['site']
		return

	if site['type'] == SiteType.DJANGO:
		root = get_django_setting(site, 'WEBAPP_ROOT')
		if not root:
			return
		prefix = get_webapp_url(site)
	else:
		if not site.has_key('webapp'):
			return
		root = site['webapp']['root']
		prefix = None

	# Build the webapp if needed
	task = site['webapp']['build_task'] if site.has_key('webapp') and site['webapp'].has_key('build_task') else None
	build_webapp(root, task)
	# Don't go any further if there is no bucket specified
	if not site.has_key('webapp') or not site['webapp'].has_key('bucket'):
		return
	# Clear the bucket
	if site['webapp'].get('clear'):
		delete_from_s3(site, site['webapp']['bucket'], prefix=prefix)
	# Upload the webapp files
	upload_to_s3(site, site['webapp']['bucket'], root, prefix=prefix)
	# Upload the static files found in other directories with the static file finder
	if site['webapp'].get('files'):
		if site['type'] == SiteType.DJANGO:
			for filename in site['webapp']['files']:
				result = find_static(site, filename)
				if not result:
					print red('Error: Could not find static file %s.' % filename)
					return
				upload_to_s3(site, site['webapp']['bucket'], result[:-len(filename)], (filename,), prefix=prefix)
		else:
			if type(site['webapp']['files']) is dict:
				for prefix in site['webapp']['files']:
					upload_to_s3(site, site['webapp']['bucket'], None, site['webapp']['files'][prefix], prefix=prefix)
			else:
				upload_to_s3(site, site['webapp']['bucket'], None, site['webapp']['files'], prefix=prefix)
