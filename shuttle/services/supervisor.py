import tempfile

from fabric.api import put, sudo
from fabric.contrib.files import append

from .service import Service
from ..formats import format_ini
from ..hooks import hook
from ..shared import pip_install, get_template_dir

class Supervisor(Service):
	name = 'supervisor'
	script = 'supervisor'

	def install(self):
		with hook('install %s' % self.name, self):
			pip_install(self.name)
			# To run automatically at startup with ubuntu and other systems:
			# http://serverfault.com/questions/96499/how-to-automatically-start-supervisord-on-linux-ubuntu
			put('%s/supervisor-upstart.conf' % get_template_dir(), '/etc/init/supervisor.conf', use_sudo=True, mode=0644)

	def config(self):
		with hook('config %s' % self.name, self):
			if self.settings:
				import StringIO
				put(StringIO.StringIO(format_init(self.settings)), '/etc/supervisor.conf', use_sudo=True, mode=0644)
		self.restart()
