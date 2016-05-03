from importlib import import_module
import tempfile

from fabric.api import run, sudo, cd, put, env, settings
from fabric.context_managers import shell_env
from fabric.contrib.files import append

from .service import Service
from ..hooks import hook
from ..shared import apt_get_install, pip_install, find_service

_POSTGRES_USER = 'postgres'

class Postgres(Service):
	name = 'postgres'
	script = 'postgresql'

	def __init__(self, settings=None, postgis=False):
		self.settings = settings
		self.postgis = postgis

	def install(self):
		with hook('install %s' % self.name, self):
			apt_get_install('postgresql')
			if self.postgis:
				self.install_postgis()

	def install_postgis(self):
		apt_get_install('postgresql-server-dev-all', 'libpq-dev', 'libxml2', 'libxml2-dev')

		# Install geos
		run('wget --no-clobber http://archive.ubuntu.com/ubuntu/pool/universe/g/geos/geos_3.3.3.orig.tar.gz')
		run('tar -xzf geos_3.3.3.orig.tar.gz')
		with cd('geos-3.3.3'):
			run('./configure')
			run('make')
			sudo('make install')

		# Install proj.4
		# https://docs.djangoproject.com/en/dev/ref/contrib/gis/install/
		run('wget --no-clobber http://download.osgeo.org/proj/proj-4.8.0.tar.gz')
		run('wget --no-clobber http://download.osgeo.org/proj/proj-datumgrid-1.5.tar.gz')
		run('tar -xzf proj-4.8.0.tar.gz')
		with cd('proj-4.8.0/nad'):
			run('tar -xzf ../../proj-datumgrid-1.5.tar.gz')
			with cd('..'):
				run('./configure')
				run('make')
				sudo('make install')

		# Install gdal
		run('wget --no-clobber http://download.osgeo.org/gdal/gdal-1.9.2.tar.gz')
		run('tar -xzf gdal-1.9.2.tar.gz')
		with cd('gdal-1.9.2'):
			run('./configure')
			run('make')
			sudo('make install')
			sudo('ldconfig')

		run('wget --no-clobber http://download.osgeo.org/postgis/source/postgis-2.0.3.tar.gz')
		run('tar -xzf postgis-2.0.3.tar.gz')
		with cd('postgis-2.0.3'):
			run('./configure')
			run('make')
			sudo('make install')

	def config(self):
		with hook('config %s' % self.name, self):
			if self.settings:
				with tempfile.NamedTemporaryFile('w') as f:
					for setting in self.settings:
						f.write('%s = %s\n' % (setting, self.settings[setting]))
					f.flush()
					put(f.name, '/etc/postgresql/9.1/main/fabric.conf', use_sudo=True, mode=0644)
					sudo('chown %s:%s /etc/postgresql/9.1/main/fabric.conf' % (_POSTGRES_USER, _POSTGRES_USER))
					append('/etc/postgresql/9.1/main/postgresql.conf', "\n\ninclude 'fabric.conf'\n", use_sudo=True)
		self.restart()

	def site_install(self, site):
		with hook('site install %s' % self.name, self, site):
			if self.postgis:
				# Install PostGIS also on the site if separate from the server
				if find_service(self.name) is None:
					self.install_postgis()
			# Install python postgresql support
			apt_get_install('python-dev', 'postgresql-server-dev-all', 'postgresql-client')
			pip_install('psycopg2')

	def site_config(self, site):
		answer = raw_input('Running this postgres site configuration will drop any existing database. Are you sure? (Y/N): ')
		if answer.lower() != 'y':
			return
		answer = raw_input('Are you absolutely sure? (Y/N): ')
		if answer.lower() != 'y':
			return
		with hook('site config %s' % self.name, self, site):
			# Create the user for django to access the database with
			module = import_module(site['settings_module'])
			DATABASES = module.DATABASES
			remote_db = True
			if find_service(self.name) is None:
				# For a remote database
				connect_args = '--username=%s --host=%s' % (DATABASES['default']['USER'], DATABASES['default']['HOST'])
				if DATABASES['default'].get('PORT'):
					connect_args += ' --port=%s' % str(DATABASES['default']['PORT'])
				user = None
			else:
				# For a local database setup the users
				connect_args = ''
				user = _POSTGRES_USER
				with settings(warn_only=True):
					sudo('createuser --createdb --no-superuser --no-createrole %s' % DATABASES['default']['USER'], user=user)
					sudo("psql -c \"ALTER USER %s WITH PASSWORD '%s';\"" % (DATABASES['default']['USER'], DATABASES['default']['PASSWORD']), user=user)
					sudo('createuser --createdb --no-superuser --no-createrole %s' % env.user, user=user)
			with shell_env(PGPASSWORD=DATABASES['default']['PASSWORD']):
				with settings(warn_only=True):
					sudo('dropdb %s %s' % (connect_args, DATABASES['default']['NAME']), user=user)
				sudo('createdb %s %s' % (connect_args, DATABASES['default']['NAME']), user=user)
				if self.postgis:
					sudo("psql %s %s -c 'CREATE EXTENSION postgis;'" % (connect_args, DATABASES['default']['NAME']), user=user)
					sudo("psql %s %s -c 'CREATE EXTENSION postgis_topology;'" % (connect_args, DATABASES['default']['NAME']), user=user)
					# http://tmbu.blogspot.com/2012/11/postgres-postgis-django-os-x-and.html
					# http://stackoverflow.com/questions/4737982/django-avoids-creating-pointfield-in-the-database-when-i-run-python-manage-py-sy
					sudo("psql %s %s -c 'GRANT ALL PRIVILEGES ON geometry_columns TO %s;'" % (connect_args, DATABASES['default']['NAME'], DATABASES['default']['USER']), user=user)
					sudo("psql %s %s -c 'GRANT ALL PRIVILEGES ON spatial_ref_sys TO %s;'" % (connect_args, DATABASES['default']['NAME'], DATABASES['default']['USER']), user=user)
