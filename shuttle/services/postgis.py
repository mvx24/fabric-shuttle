from fabric.api import run, sudo, cd

from ..shared import apt_get_install, get_django_setting

_GEOS_URL = 'http://archive.ubuntu.com/ubuntu/pool/universe/g/geos/geos_3.3.3.orig.tar.gz'
_GEOS_DIR = 'geos-3.3.3'
_PROJ4_URL = 'http://download.osgeo.org/proj/proj-4.8.0.tar.gz'
_PROJ4_DIR = 'proj-4.8.0/nad'
_PROJ4_DATUM_URL = 'http://download.osgeo.org/proj/proj-datumgrid-1.5.tar.gz'
_GDAL_URL = 'http://download.osgeo.org/gdal/gdal-1.9.2.tar.gz'
_GDAL_DIR = 'gdal-1.9.2'
_POSTGIS_URL = 'http://download.osgeo.org/postgis/source/postgis-2.0.3.tar.gz'
_POSTGIS_DIR = 'postgis-2.0.3'

def install_postgis():
	apt_get_install('postgresql-server-dev-all', 'libpq-dev', 'libxml2', 'libxml2-dev')

	# Install geos
	run('wget --no-clobber %s' % _GEOS_URL)
	run('tar -xzf %s' % _GEOS_URL.split('/')[-1])
	with cd(_GEOS_DIR):
		run('./configure')
		run('make')
		sudo('make install')

	# Install proj.4
	# https://docs.djangoproject.com/en/dev/ref/contrib/gis/install/
	run('wget --no-clobber %s' % _PROJ4_URL)
	run('wget --no-clobber %s' % _PROJ4_DATUM_URL)
	run('tar -xzf %s' % _PROJ4_URL.split('/')[-1])
	with cd(_PROJ4_DIR):
		run('tar -xzf ../../%s' % _PROJ4_DATUM_URL.split('/')[-1])
		with cd('..'):
			run('./configure')
			run('make')
			sudo('make install')

	# Install gdal
	run('wget --no-clobber %s' % _GDAL_URL)
	run('tar -xzf %s' % _GDAL_URL.split('/')[-1])
	with cd(_GDAL_DIR):
		run('./configure')
		run('make')
		sudo('make install')
		sudo('ldconfig')

	run('wget --no-clobber %s' % _POSTGIS_URL)
	run('tar -xzf %s' % _POSTGIS_URL.split('/')[-1])
	with cd(_POSTGIS_DIR):
		run('./configure')
		run('make')
		sudo('make install')

def site_config_postgis(postgres, site):
	database = get_django_setting(site, 'DATABASES')['default']
	postgres.execute_sql('CREATE EXTENSION postgis;', site)
	postgres.execute_sql('CREATE EXTENSION postgis_topology;', site)
	# http://tmbu.blogspot.com/2012/11/postgres-postgis-django-os-x-and.html
	# http://stackoverflow.com/questions/4737982/django-avoids-creating-pointfield-in-the-database-when-i-run-python-manage-py-sy
	postgres.execute_sql('GRANT ALL PRIVILEGES ON geometry_columns TO %s;' % database['USER'], site)
	postgres.execute_sql('GRANT ALL PRIVILEGES ON spatial_ref_sys TO %s;' % database['USER'], site)
