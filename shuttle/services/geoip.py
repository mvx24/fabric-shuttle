from fabric.api import sudo, run
from fabric.contrib.files import exists

from .service import Service
from ..hooks import hook
from ..shared import apt_get_install, pip_install

class GeoIP(Service):
	"""Database to convert ip addresses into locations."""
	name = 'geoip'
	script = None

	def install(self):
		with hook('install %s' % self.name, self):
			if not exists('/usr/local/geoip'):
				apt_get_install('libgeoip1')
				run('wget --no-clobber http://www.maxmind.com/download/geoip/database/GeoLiteCity.dat.gz')
				run('gunzip GeoLiteCity.dat.gz')
				sudo('mkdir /usr/local/geoip')
				sudo('mv GeoLiteCity.dat /usr/local/geoip/')

	def site_install(self, site):
		with hook('site install %s' % self.name, self, site):
			pip_install()
