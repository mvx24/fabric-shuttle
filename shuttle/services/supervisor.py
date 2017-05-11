import tempfile

from fabric.api import put, sudo
from fabric.contrib.files import append, sed, exists

from .service import Service
from ..formats import format_ini
from ..hooks import hook
from ..shared import WWW_USER, pip_install, get_template, get_project_directory, get_python_interpreter, chown

_CONFIG_FILE = '/etc/supervisor/supervisor.conf'

django_management_program = {
	'numprocs': '4',
	'process_name': '%(program_name)s_%(process_num)02d',
	'autostart': 'true',
	'autorestart': 'true',
	'user': WWW_USER
}

# NOTE: supervisor doesn't support quotes around the ini values in config files

class Supervisor(Service):
	name = 'supervisor'
	script = 'supervisor'

	def install(self):
		with hook('install %s' % self.name, self):
			pip_install(None, self.name)
			# To run automatically at startup with ubuntu and other systems:
			# http://serverfault.com/questions/96499/how-to-automatically-start-supervisord-on-linux-ubuntu
			chown(put(get_template('supervisor-upstart.conf'), '/etc/init/supervisor.conf', use_sudo=True, mode=0644))
			# https://wiki.ubuntu.com/SystemdForUpstartUsers
			if exists('/lib/systemd/system', use_sudo=True):
				chown(put(get_template('supervisor-systemd.conf'), '/lib/systemd/system/supervisor.service', use_sudo=True, mode=0644))
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
				chown(put(StringIO.StringIO(format_ini(self.settings, quotes=False)), '/etc/supervisor/supervisor-fabric.conf', use_sudo=True, mode=0644))
				append(_CONFIG_FILE, '[include]', use_sudo=True)
				append(_CONFIG_FILE, 'files=/etc/supervisor/supervisor-fabric.conf', use_sudo=True)
		self.restart()

	def site_config(self, site):
		# Any supervisor configuration can be used but shortcuts are provided for Django
		# If settings has 'program:name': 'management command' this will be expanded with default settings and correctly resolved to the project/virtualenv
		# Likewise if a 'program:name': { 'command': 'manage.py management command', ... } is used this will also be resolved to the project/virtualenv
		with hook('site config %s' % self.name, self, site):
			if self.settings:
				# Process shortcuts on setting up rq management commands
				for program, config in self.settings.items():
					if program.startswith('program:'):
						if isinstance(config, (str, unicode)):
							command = config
							config = self.settings[program] = django_management_program.copy()
							config['command'] = 'manage.py ' + command
						# Expand a manage.py command to use the correct python interpreter
						command = config.get('command', '')
						if command.startswith('manage.py'):
							config['command'] = get_python_interpreter() + ' ' + command
							config['directory'] = get_project_directory()
				import StringIO
				file_name = '/etc/supervisor/%s.conf' % site['name']
				chown(put(StringIO.StringIO(format_ini(self.settings, quotes=False)), file_name, use_sudo=True, mode=0644))
				append(_CONFIG_FILE, '[include]', use_sudo=True)
				append(_CONFIG_FILE, 'files=%s' % file_name, use_sudo=True)
		self.restart()
