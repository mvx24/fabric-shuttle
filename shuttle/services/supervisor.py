import tempfile

from fabric.api import put, sudo
from fabric.contrib.files import append, sed

from .service import Service
from ..formats import format_ini
from ..hooks import hook
from ..shared import pip_install, get_template_dir

_CONFIG_FILE = '/etc/supervisor/supervisor.conf'

# NOTE: supervisor doesn't support quotes around the ini values in config files

class Supervisor(Service):
	name = 'supervisor'
	script = 'supervisor'

	def install(self):
		with hook('install %s' % self.name, self):
			pip_install(self.name)
			# To run automatically at startup with ubuntu and other systems:
			# http://serverfault.com/questions/96499/how-to-automatically-start-supervisord-on-linux-ubuntu
			put('%s/supervisor-upstart.conf' % get_template_dir(), '/etc/init/supervisor.conf', use_sudo=True, mode=0644)
			# Start the default configuration, with logging and pid file location changed to be more like nginx and other services
			sudo('mkdir -p /etc/supervisor')
			sudo('mkdir -p /var/log/supervisor')
			sudo('echo_supervisord_conf > ' + _CONFIG_FILE)
			sed(_CONFIG_FILE, '/tmp/supervisord.log', '/var/log/supervisor/supervisor.log', use_sudo=True)
			sed(_CONFIG_FILE, '/tmp/supervisord.pid', '/var/run/supervisor/supervisor.pid', use_sudo=True)
			sed(_CONFIG_FILE, '/tmp/supervisor.sock', '/var/run/supervisor/supervisor.sock', use_sudo=True)

	def config(self):
		with hook('config %s' % self.name, self):
			if self.settings:
				import StringIO
				put(StringIO.StringIO(format_ini(self.settings, quotes=False)), '/etc/supervisor/supervisor-fabric.conf', use_sudo=True, mode=0644)
				append(_CONFIG_FILE, '[include]', use_sudo=True)
				append(_CONFIG_FILE, 'files=/etc/supervisor/supervisor-fabric.conf', use_sudo=True)
		self.restart()

	def site_config(self, site):
		with hook('site config %s' % self.name, self, site):
			if self.settings:
				import StringIO
				file_name = '/etc/supervisor/%s.conf' % site['name']
				put(StringIO.StringIO(format_ini(self.settings, quotes=False)), file_name, use_sudo=True, mode=0644)
				append(_CONFIG_FILE, '[include]', use_sudo=True)
				append(_CONFIG_FILE, 'files=%s' % file_name, use_sudo=True)
		self.restart()
