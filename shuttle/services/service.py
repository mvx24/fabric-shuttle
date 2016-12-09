from fabric.api import sudo

from ..hooks import hook

class Service(object):

	def __init__(self, **kwargs):
		self.settings = {} if not kwargs else kwargs

	def install(self):
		pass

	def config(self):
		pass

	def site_install(self, site):
		pass

	def site_config(self, site):
		pass

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
