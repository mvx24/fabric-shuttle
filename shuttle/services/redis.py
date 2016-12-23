import tempfile

from fabric.api import put, sudo
from fabric.contrib.files import append

from .service import Service
from ..hooks import hook
from ..shared import apt_get_install, pip_install, chown

class Redis(Service):
	name = 'redis'
	script = 'redis-server'

	def install(self):
		with hook('install %s' % self.name, self):
			apt_get_install('redis-server')

	def config(self):
		with hook('config %s' % self.name, self):
			if self.settings:
				with tempfile.NamedTemporaryFile('w') as f:
					for setting in self.settings:
						if self.settings[setting]:
							f.write('%s %s\n' % (setting, self.settings[setting]))
						else:
							f.write('%s\n' % setting)
					f.flush()
					chown(put(f.name, '/etc/redis/fabric.conf', use_sudo=True, mode=0644))
					append('/etc/redis/redis.conf', 'include /etc/redis/fabric.conf', use_sudo=True)
		self.restart()

	def site_install(self, site):
		with hook('site install %s' % self.name, self, site):
			pip_install(site, 'redis', 'django-redis')
