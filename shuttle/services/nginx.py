from importlib import import_module
import os
import tempfile
import urlparse

from fabric.api import put, sudo, hide, settings
from fabric.contrib.files import upload_template

from .service import Service
from ..shared import apt_get_install, get_template_dir, fix_static_path, fix_webapp_path, SiteType
from ..hooks import hook

NGINX_USER = 'www-data'
_NGINX_SSL = """listen 443 ssl;\n\tssl_certificate %s;\n\tssl_certificate_key %s;"""
_NGINX_LOCATION = """location %s {\n\t\talias %s;\n\t\texpires 1d;\n\t}"""
_NGINX_LOCATION_DOMAIN = """location %s {\n\t\trewrite ^(.*)$ http://%s$1 permanent;\n\t}"""
_NGINX_WEBAPP_LOCATION = """location %s {\n\t\talias %s;\n\t\ttry_files $uri $uri/%s %s;\n\t}"""

def _get_domain(url):
	return urlparse.urlparse(url).netloc

def _get_path(url):
	return urlparse.urlparse(url).path

def _slash_wrap(path):
	if path.startswith('/'):
		if path.endswith('/'):
			return path
		else:
			return path + '/'
	else:
		if path.endswith('/'):
			return '/' + path
		else:
			return '/%s/' % path

class Nginx(Service):
	name = 'nginx'
	script = 'nginx'

	def install(self):
		with hook('install %s' % self.name, self):
			apt_get_install('nginx-full')

	def config(self):
		with hook('config %s' % self.name, self):
			if self.settings:
				with tempfile.NamedTemporaryFile('w') as f:
					for section in self.settings:
						if type(self.settings[section]) is dict:
							f.write('%s {\n' % section)
							for setting in self.settings[section]:
								f.write('%s %s;\n' % (setting, self.settings[section][setting]))
							f.write('}\n')
						elif type(self.settings[section]) is bool:
							f.write('%s %s;\n' % (section, 'on' if self.settings[section] else 'off'))
						else:
							f.write('%s %s;\n' % (section, str(self.settings[section])))
					f.flush()
					put(f.name, '/etc/nginx/conf.d/fabric.conf', use_sudo=True, mode=0644)		
					sudo('chown root:root /etc/nginx/conf.d/fabric.conf')
		self.restart()

	def site_config(self, site):
		with hook('site config %s' % self.name, self, site):
			context = {
				'site': site['name'],
				'default_str': ' default_server' if self.settings.get('default') else '',
				'app_location': '/',
				'webapp_location': '',
				'webapp_404': '',
			}
			if self.settings.has_key('log_level'):
				context['log_level'] = ' ' + self.settings['log_level']
			else:
				context['log_level'] = ''
			if self.settings.get('ssl_cert') and self.settings.get('ssl_cert_key'):
				context['ssl_str'] = _NGINX_SSL % (self.settings['ssl_cert'], self.settings['ssl_cert_key'])
			else:
				context['ssl_str'] = ''
			if self.settings.has_key('location_settings') and isinstance(self.settings['location_settings'], (list, tuple)):
				context['location_settings_str'] = '\n\t\t'.join(['%s %s;' % setting for setting in self.settings['location_settings']])
			else:
				context['location_settings_str'] = ''
			
			try:
				module = import_module(site.get('settings_module'))
			except:
				module = None
			
			if module:
				# Django site setup
				context['location_settings_str'] = '\n\t\t'.join((context['location_settings_str'], 'uwsgi_pass unix:///var/run/uwsgi/app/%s/socket;' % site['name'], 'include uwsgi_params;'))
				if module.ALLOWED_HOSTS:
					context['allowed_hosts'] = ' '.join(module.ALLOWED_HOSTS)
				else:
					context['allowed_hosts'] = site['name']
				# Setup the static and media locations
				locations = []
				domain = _get_domain(module.STATIC_URL)
				if domain:
					locations.append(_NGINX_LOCATION_DOMAIN % (_slash_wrap(_get_path(module.STATIC_URL)), domain))
				else:
					locations.append(_NGINX_LOCATION % (_slash_wrap(module.STATIC_URL), fix_static_path(module.STATIC_ROOT, site)))
				domain = _get_domain(module.MEDIA_URL)
				if domain:
					locations.append(_NGINX_LOCATION_DOMAIN % (_slash_wrap(_get_path(module.MEDIA_URL)), domain))
				else:
					locations.append(_NGINX_LOCATION % (_slash_wrap(module.MEDIA_URL), fix_static_path(module.MEDIA_ROOT, site)))
				# Add any custom locations
				if self.settings.has_key('custom_locations'):
					if isinstance(self.settings['custom_locations'], (tuple, list)):
						for location in self.settings['custom_locations']:
							locations.append(location)
					else:
						locations.append(self.settings['custom_locations'])
				context['static_locations'] = '\n\n\t'.join(locations)
				# Configure the webapp if necessary
				webapp_root = getattr(module, 'WEBAPP_ROOT', None)
				webapp_url = _slash_wrap(getattr(module, 'WEBAPP_URL', '/'))
				if webapp_root:
					webapp_root = fix_webapp_path(webapp_root)
					webapp_index = getattr(module, 'WEBAPP_INDEX', 'index.html')
					if webapp_url == '/':
						context['app_location'] = '@%s-app' % site['name'].replace('.', '-')
						context['webapp_location'] = _NGINX_WEBAPP_LOCATION % (webapp_url, webapp_root, webapp_index, context['app_location'])
					else:
						context['app_location'] = '/'
						context['webapp_location'] = _NGINX_WEBAPP_LOCATION % (webapp_url, webapp_root, webapp_index, '=404')
					# Configure the 404 file if available
					if os.path.isfile(os.path.join(module.WEBAPP_ROOT, '404.html')):
						context['webapp_404'] = 'error_page 404 %s404.html;' % webapp_url
					else:
						context['webapp_404'] = ''
			else:
				# Not a django site
				context['allowed_hosts'] = site['name']
				context['static_locations'] = ''

			if self.settings.get('ssl_only'):
				nginx_template = '%s/nginx-site-https.conf' % get_template_dir()
			else:
				nginx_template = '%s/nginx-site.conf' % get_template_dir()
			upload_template(nginx_template, '/etc/nginx/sites-available/%s.conf' % site['name'], context=context, use_sudo=True, mode=0644)
			sudo('chown root:root /etc/nginx/sites-available/%s.conf' % site['name'])
			# If site type is NGINX enable it right away because there is no deployment process for it
			if site['type'] == SiteType.NGINX:
				with hide('warnings'), settings(warn_only=True):
					sudo('ln -s /etc/nginx/sites-available/%s.conf /etc/nginx/sites-enabled/%s.conf' % (site['name'], site['name']))
		self.restart()
