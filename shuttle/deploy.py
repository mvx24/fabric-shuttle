from importlib import import_module
import os
import subprocess

from fabric.api import sudo, local, put, settings

from .services.nginx import NGINX_USER
from .services.s3 import upload_to_s3, delete_from_s3
from .shared import *

class own_project(object):
	"""Changes the permissions of the project between the ssh user and nginx user."""
	def __init__(self):
		pass
	def __enter__(self):
		sudo('chown -R %s:%s /srv/www/%s' % (env['user'], env['user'], env['project']))
	def __exit__(self, *_):
		sudo('chown -R %s:%s /srv/www/%s' % (NGINX_USER, NGINX_USER, env['project']))

def _django_get_excluded(sites):
	"""Get an excluded list of patterns.

	rsync patterns are similar to .gitignore patterns. e.g. /foo matches foo at the base.
	http://git-scm.com/docs/gitignore
	http://ss64.com/bash/rsync.html
	"""
	excluded = ['.*', '*.pyc', '*.db']
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
		module = import_module(site['settings_module'])
		os.environ.setdefault("DJANGO_SETTINGS_MODULE", site['settings_module'])
		if hasattr(module, 'WEBAPP_ROOT'):
			webapp_root = module.WEBAPP_ROOT
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

SYNC_COMMAND = 'rsync -avz %s --delete --delete-excluded%s ./ %s@%s:/srv/www/%s'

def django_sync(sites):
	"""Syncs the local code to the server. Used as part of the deploy process. Wrap in the task decorator to enable. sync = task(sync)"""
	sudo('mkdir -p /srv/www/%s' % env['project'])
	with own_project():
		# Preserve existing media if a subdirectory of the project
		for site in sites:
			module = import_module(site['settings_module'])
			MEDIA_ROOT = fix_static_path(module.MEDIA_ROOT, site)
			if MEDIA_ROOT.startswith('/srv/www/%s/' % env['project']) and exists(MEDIA_ROOT):
				sudo('mv %s /srv/www/%smedia' % (MEDIA_ROOT, env['project']))

		# Sync and set the owner
		excluded = _django_get_excluded(sites)
		excluded = ['--exclude="%s"' % ex for ex in excluded]
		local(SYNC_COMMAND % (' '.join(excluded), ' -e "ssh -i %s"' % os.path.expanduser(env['key_filename']) if env.get('key_filename') else '', env['user'], env['hosts'][0], env['project']))

		# Create the media directory
		# Restore existing media if a subdirectory of the project, if it is outside the project make sure it is owned by nginx
		for site in sites:
			module = import_module(site['settings_module'])
			MEDIA_ROOT = fix_static_path(module.MEDIA_ROOT, site)
			sudo('mkdir -p %s' % MEDIA_ROOT)
			if MEDIA_ROOT.startswith('/srv/www/%s/' % env['project']):
				if exists('/srv/www/%smedia' % env['project']):
					# Delete the media directory but leave intermediate directories then restore with a move
					sudo('rm -rf %s' % MEDIA_ROOT)
					sudo('mv /srv/www/%smedia %s' % (env['project'], MEDIA_ROOT))
			else:
				sudo('chown -R %s:%s %s' % (NGINX_USER, NGINX_USER, MEDIA_ROOT))

			# Copy any additional webapp files
			if site.has_key('webapp') and site['webapp'].get('files'):
				from django.contrib.staticfiles import finders
				if hasattr(module, 'WEBAPP_ROOT'):
					WEBAPP_ROOT = fix_webapp_path(module.WEBAPP_ROOT)
					for filename in site['webapp']['files']:
						result = finders.find(filename)
						if not result:
							print red('Error: Could not find static file %s.' % filename)
							return
						result = result[0] if isinstance(result, (list, tuple)) else result
						put(result, os.path.join(WEBAPP_ROOT, filename), use_sudo=True, mode=0644)

def django_sync_dry_run(sites):
	"""Do an rsync dry run to see which files will be updated when deploying."""
	# e.g. To show just migrations: fab e:production x:dryrun | grep -v "^deleting" | grep -v "/$" | grep "^shared/migrations"
	excluded = _django_get_excluded(sites)
	excluded = ['--exclude="%s"' % ex for ex in excluded]
	local(SYNC_COMMAND % (' '.join(excluded), ' --dry-run -e "ssh -i %s"' % os.path.expanduser(env['key_filename']) if env.get('key_filename') else '', env['user'], env['hosts'][0], env['project']))

def django_sync_down(path=''):
	"""Syncs down stuff from the server. Wrap in the task decorator to enable. sync_down = task(sync_down)"""
	if path.startswith('/'):
		path = path[1:]
	if not path.endswith('/'):
		path += '/'
	local('rsync -avz --exclude=".*" --exclude="*.pyc" --exclude="*.sh" --exclude="*.db" %s@%s:/srv/www/%s%s ./%s' % (env['user'], env['hosts'][0], env['project'], path, path))

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
		try:
			module = import_module(site['settings_module'])
			root = getattr(module, 'WEBAPP_ROOT', None)
			prefix = getattr(module, 'WEBAPP_URL', None)
			if not root:
				return
		except:
			print red('Error: Could not import webapp settings to deploy.')
			return
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
			from django.contrib.staticfiles import finders
			for filename in site['webapp']['files']:
				result = finders.find(filename)
				if not result:
					print red('Error: Could not find static file %s.' % filename)
					return
				result = result[0] if isinstance(result, (list, tuple)) else result
				upload_to_s3(site, site['webapp']['bucket'], result[:-len(filename)], (filename,), prefix=prefix)
		else:
			if type(site['webapp']['files']) is dict:
				for prefix in site['webapp']['files']:
					upload_to_s3(site, site['webapp']['bucket'], None, site['webapp']['files'][prefix], prefix=prefix)
			else:
				upload_to_s3(site, site['webapp']['bucket'], None, site['webapp']['files'], prefix=prefix)
