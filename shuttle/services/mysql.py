import tempfile

from fabric.api import put, sudo

from .service import Service
from ..hooks import hook
from ..shared import apt_get_install, pip_install

class MySQL(Service):
	name = 'mysql'
	script = 'mysql'

	def install(self):
		with hook('install %s' % self.name, self):
			apt_get_install('mysql-server')

	def config(self):
		with hook('config %s' % self.name, self):
			if self.settings:
				with tempfile.NamedTemporaryFile('w') as f:
					for section in self.settings:
						f.write('[%s]\n' % section)
						for setting in self.settings[section]:
							f.write('%s = %s\n' % (setting, self.settings[section][setting]))
					f.flush()
					put(f.name, '/etc/mysql/conf.d/fabric.cnf', use_sudo=True, mode=0644)		
					sudo('chown root:root /etc/mysql/conf.d/fabric.cnf')
		self.restart()

	def site_install(self, site):
		with hook('site install %s' % self.name, self, site):
			apt_get_install('mysql-client', 'libmysqlclient-dev')
			pip_install('mysql-python')
