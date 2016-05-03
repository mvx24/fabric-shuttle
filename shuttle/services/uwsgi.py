import tempfile, os

from fabric.api import env, sudo, settings
from fabric.contrib.files import upload_template

from .service import Service
from ..hooks import hook
from ..shared import apt_get_install, get_template_dir

class UWSGI(Service):
	name = 'uwsgi'
	script = 'uwsgi'

	def install(self):
		with hook('install %s' % self.name, self):
			apt_get_install('uwsgi', 'uwsgi-plugin-python')

	def get_custom_settings(self):
		lines = []
		for key, value in self.settings.items():
			if isinstance(value, bool):
				value = str(value).lower()
			lines.append('%s = %s' % (key, value))
		return '\n'.join(lines)

	def site_config(self, site):
		with hook('site config %s' % self.name, self, site):
			wsgi_module = site['wsgi_module'] if site.has_key('wsgi_module') else site['settings_module'].split('.')[0] + '.wsgi'
			context = {'project': env['project'], 'settings_module': site['settings_module'], 'wsgi_module': wsgi_module, 'custom_settings': self.get_custom_settings()}
			upload_template('%s/uwsgi-app.ini' % get_template_dir(), '/etc/uwsgi/apps-available/%s.ini' % site['name'], context=context, use_sudo=True, mode=0644)
			sudo('chown root:root /etc/uwsgi/apps-available/%s.ini' % site['name'])
		self.restart()
