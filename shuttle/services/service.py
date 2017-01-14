from fabric.api import sudo

from ..hooks import hook

class Service(object):

	def __init__(self, *args, **kwargs):
		self.settings = {}
		for settings_dict in args:
			self.settings.update(settings_dict)
		self.settings.update(kwargs)

	def copy(self, **kwargs):
		c = self.__class__(**self.settings)
		c.settings.update(kwargs)
		return c

	def __copy__(self):
		return self.copy()

	def __deepcopy__(self, memo):
		return self.copy()

	# All the setup commands

	def install(self):
		pass

	def config(self):
		pass

	def site_install(self, site):
		pass

	def site_config(self, site):
		pass

	# Functions to alter the service running state

	def start(self):
		if hasattr(self, 'script'):
			with hook('start %s' % self.name, self):
				sudo('service %s start' % self.script, pty=False)

	def stop(self):
		if hasattr(self, 'script'):
			with hook('stop %s' % self.name, self):
				sudo('service %s stop' % self.script, pty=False)

	def restart(self):
		if hasattr(self, 'script'):
			with hook('restart %s' % self.name, self):
				sudo('service %s restart' % self.script, pty=False)

	# Additional settings to use when deploying the site

	def get_site_settings(self, site):
		return {}
