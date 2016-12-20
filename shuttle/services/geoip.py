from fabric.api import cd, sudo
from fabric.contrib.files import exists

from .service import Service
from ..hooks import hook
from ..shared import apt_get_install, pip_install, SiteType

# http://dev.maxmind.com/geoip/legacy/geolite/
_CITY_PACKAGE_URL = 'http://geolite.maxmind.com/download/geoip/database/GeoLiteCity.dat.gz'
_COUNTRY_PACKAGE_URL = 'http://geolite.maxmind.com/download/geoip/database/GeoLiteCountry/GeoIP.dat.gz'
_INSTALL_DIR = '/opt/geoip'

class GeoIP(Service):
	"""Database to convert ip addresses into locations."""
	name = 'geoip'
	script = None

	def install(self):
		with hook('install %s' % self.name, self):
			if not exists(_INSTALL_DIR):
				apt_get_install('libgeoip1')
				sudo('mkdir %s' % _INSTALL_DIR)
				with cd(_INSTALL_DIR):
					sudo('wget --no-clobber %s' % _CITY_PACKAGE_URL)
					sudo('wget --no-clobber %s' % _COUNTRY_PACKAGE_URL)
					sudo('gunzip %s' % _CITY_PACKAGE_URL.split('/')[-1])
					sudo('gunzip %s' % _COUNTRY_PACKAGE_URL.split('/')[-1])

	def get_site_settings(self, site):
		if site['type'] == SiteType.DJANGO:
			return { 'GEOIP_PATH': _INSTALL_DIR }
		else:
			return super(GeoIP, self).get_site_settings(site)
