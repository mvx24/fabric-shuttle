import os
import tempfile
import urlparse

from fabric.api import put, sudo, hide, settings, env
from fabric.contrib.files import upload_template

from .service import Service
from ..shared import apt_get_install, get_template, fix_absolute_path, SiteType, chown, find_static
from ..shared import get_django_setting, get_static_root, get_static_url, get_media_root, get_media_url, get_webapp_root, get_webapp_url
from ..hooks import hook

_NGINX_SSL = """listen 443 ssl;\n\tssl_certificate %s;\n\tssl_certificate_key %s;"""
_NGINX_LOCATION = """location %s {\n\t\talias %s;\n\t\texpires 1d;\n\t}"""
_NGINX_LOCATION_DOMAIN = """location %s {\n\t\trewrite ^(.*)$ http://%s$1 permanent;\n\t}"""
_NGINX_WEBAPP_LOCATION = """location %s {\n\t\talias %s;\n\t\ttry_files $uri $uri/%s %s;\n\t}"""

def _get_domain(url):
	return urlparse.urlparse(url).netloc

def _get_path(url):
	return urlparse.urlparse(url).path

def _slash_append(path):
	if not path.endswith('/'):
		return path + '/'
	return path

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
						if isinstance(self.settings[section], dict):
							f.write('%s {\n' % section)
							for setting in self.settings[section]:
								f.write('\t%s %s;\n' % (setting, self.settings[section][setting]))
							f.write('}\n')
						elif type(self.settings[section]) is bool:
							f.write('%s %s;\n' % (section, 'on' if self.settings[section] else 'off'))
						else:
							f.write('%s %s;\n' % (section, str(self.settings[section])))
					f.flush()
					chown(put(f.name, '/etc/nginx/conf.d/fabric.conf', use_sudo=True, mode=0644))
		self.restart()

	def site_config(self, site):
		with hook('site config %s' % self.name, self, site):
			context = {
				'site': site['name'],
				'default_str': ' default_server' if self.settings.get('default') else '',
				'app_location': '/',
				'webapp_location': '',
				'error_page_str': '',
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

			if self.settings.has_key('server_settings') and isinstance(self.settings['server_settings'], (list, tuple)):
				context['server_settings_str'] = '\n\t\t'.join(['%s %s;' % setting for setting in self.settings['server_settings']])
			else:
				context['server_settings_str'] = ''

			if site['type'] == SiteType.DJANGO:
				# Django site setup
				context['location_settings_str'] = '\n\t\t'.join((context['location_settings_str'], 'uwsgi_pass unix:///var/run/uwsgi/app/%s/socket;' % site['name'], 'include uwsgi_params;'))
				context['allowed_hosts'] = ' '.join(get_django_setting(site, 'ALLOWED_HOSTS') or [site['name']])
				# Setup the static and media locations
				locations = []
				static_root = get_static_root(site)
				static_url = get_static_url(site)
				media_root = get_media_root(site)
				media_url = get_media_url(site)
				domain = _get_domain(static_url)
				if domain:
					locations.append(_NGINX_LOCATION_DOMAIN % (_slash_wrap(_get_path(static_url)), domain))
				else:
					locations.append(_NGINX_LOCATION % (_slash_wrap(static_url), _slash_append(static_root)))
				domain = _get_domain(media_url)
				if domain:
					locations.append(_NGINX_LOCATION_DOMAIN % (_slash_wrap(_get_path(media_url)), domain))
				else:
					locations.append(_NGINX_LOCATION % (_slash_wrap(media_url), _slash_append(media_root)))
				# Add any custom locations
				if self.settings.has_key('custom_locations'):
					if isinstance(self.settings['custom_locations'], (tuple, list)):
						for location in self.settings['custom_locations']:
							locations.append(location)
					else:
						locations.append(self.settings['custom_locations'])
				context['static_locations'] = '\n\n\t'.join(locations)
				# Configure the webapp if necessary
				webapp_root = get_webapp_root(site)
				if webapp_root:
					webapp_url = _slash_wrap(get_webapp_url(site))
					webapp_index = get_django_setting(site, 'WEBAPP_INDEX') or 'index.html'
					if webapp_url == '/':
						context['app_location'] = '@%s-app' % site['name'].replace('.', '-')
						context['webapp_location'] = _NGINX_WEBAPP_LOCATION % (webapp_url, _slash_append(webapp_root), webapp_index, context['app_location'])
					else:
						context['app_location'] = '/'
						context['webapp_location'] = _NGINX_WEBAPP_LOCATION % (webapp_url, _slash_append(webapp_root), webapp_index, '=404')
					# Configure the error pages if present
					error_pages = []
					if not env.get('vagrant'):
						for status_code in (400, 401, 403, 404, 500):
							static_name = '%d.html' % status_code
							result = find_static(site, static_name)
							if result:
								error_pages.append('error_page %d %s;' % (status_code, os.path.join(static_url, static_name)))
					context['error_page_str'] = '\n\t'.join(error_pages)
			else:
				# Not a django site
				context['allowed_hosts'] = site['name']
				context['static_locations'] = ''

			if self.settings.get('ssl_only'):
				nginx_template = get_template('nginx-site-https.conf')
			else:
				nginx_template = get_template('nginx-site.conf')
			upload_template(nginx_template, '/etc/nginx/sites-available/%s.conf' % site['name'], context=context, use_sudo=True, mode=0644)
			sudo('chown root:root /etc/nginx/sites-available/%s.conf' % site['name'])
			# If site type is NGINX enable it right away because there is no deployment process for it
			if site['type'] == SiteType.NGINX:
				with hide('warnings'), settings(warn_only=True):
					sudo('ln -sf /etc/nginx/sites-available/%s.conf /etc/nginx/sites-enabled/%s.conf' % (site['name'], site['name']))
			# If the site is the default, then remove the default that comes with nginx
			if self.settings.get('default'):
				sudo('rm -f /etc/nginx/sites-enabled/default')
		self.restart()
