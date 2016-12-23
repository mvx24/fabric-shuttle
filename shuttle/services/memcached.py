import tempfile

from fabric.api import put, sudo

from .service import Service
from ..hooks import hook
from ..shared import apt_get_install, pip_install, chown

class Memcached(Service):
	name = 'memcached'
	script = 'memcached'
	DEFAULT_SETTINGS = {'-d': None, 'logfile': '/var/log/memcached.log', '-m': '64', '-p': '11211', '-u': 'memcache', '-l': '127.0.0.1'}

	def install(self):
		with hook('install %s' % self.name, self):
			apt_get_install('memcached')

	def config(self):
		settings = self.settings if self.settings else Memcached.DEFAULT_SETTINGS
		with hook('config %s' % self.name, self):
			with tempfile.NamedTemporaryFile('w') as f:
				for setting in settings:
					if settings[setting]:
						f.write('%s %s\n' % (setting, settings[setting]))
					else:
						f.write('%s\n' % setting)
				f.flush()
				chown(put(f.name, '/etc/memcached.conf', use_sudo=True, mode=0644))
		self.restart()

	def site_install(self, site):
		with hook('site install %s' % self.name, self, site):
			pip_install(site, 'python-memcached')
