import StringIO

from fabric.api import run, sudo, cd, put, env, settings, hide
from fabric.context_managers import shell_env
from fabric.contrib.files import append, exists

from .postgis import *
from .service import Service
from ..hooks import hook
from ..shared import apt_get_install, pip_install, find_service, chown, get_django_setting

POSTGRES_USER = 'postgres'
POSTGRES_GROUP = 'postgres'

_SHOW_FILE_COMMAND = '$(sudo -u %s psql -t -P format=unaligned -c "SHOW %%s;")' % POSTGRES_USER
_CONFIG_FILE_PATH = _SHOW_FILE_COMMAND % 'config_file'
_HBA_FILE_PATH = _SHOW_FILE_COMMAND % 'hba_file'
_IDENT_FILE_PATH = _SHOW_FILE_COMMAND % 'ident_file'
_MAIN_DIR = '$(dirname %s)' % _CONFIG_FILE_PATH
_CONF_DIR = '%s/conf.d' % _MAIN_DIR
_EXCLUDE_SETTINGS = ['postgis', 'hba', 'ident']
# NOTE: If hba is set to True instead of a list, then client authentication for the current local host is added

def _pg_quote_config(key, value):
	if (isinstance(value, (str, unicode)) and value not in ('on', 'off') and not value[0].isdigit()) or key == 'listen_addresses':
		return "'%s'" % value
	return value

class Postgres(Service):
	name = 'postgres'
	script = 'postgresql'

	def execute_sql(raw_sql, site=None):
		sql = []
		for line in raw_sql.split('\n'):
			line = line.strip()
			if not line or line.startswith('--'):
				continue
			sql.append(line.replace('\t', '').replace('\n', '').replace("'", "\\'"))
		sql = ' '.join(sql)
		if site:
			database = get_django_setting(site, 'DATABASES')['default']
			pg_env = {
				'PGHOST': database['HOST'],
				'PGPORT': str(database.get('PORT') or '5432'),
				'PGUSER': database['USER'],
				'PGPASSWORD': database['PASSWORD'],
				'PGDATABASE': database['NAME']
			}
			with shell_env(**pg_env), settings(warn_only=True):
				sudo("psql -c $'%s'" % sql)
		else:
			with settings(warn_only=True):
				sudo("psql -c $'%s'" % sql, user=POSTGRES_USER)

	def install(self):
		with hook('install %s' % self.name, self):
			apt_get_install('postgresql')
			if self.settings.get('postgis'):
				install_postgis()

	def config(self):
		with hook('config %s' % self.name, self):
			if not exists(_CONF_DIR):
				sudo('mkdir %s' % _CONF_DIR)
				chown(_CONF_DIR, POSTGRES_USER, POSTGRES_GROUP)
				append(_CONFIG_FILE_PATH, "include_dir 'conf.d'", use_sudo=True)
			if self.settings:
				# Apply any given settings and place into a new conf.d directory
				config = ''
				if env.get('vagrant'):
					self.settings['listen_addresses'] = '*'
				for setting in self.settings:
					if setting not in _EXCLUDE_SETTINGS:
						config += '%s = %s\n' % (setting, _pg_quote_config(setting, self.settings[setting]))
				if config:
					chown(put(StringIO.StringIO(config), _CONF_DIR + '/fabric.conf', use_sudo=True, mode=0644), POSTGRES_USER, POSTGRES_GROUP)
				# Apply any given Client Authentications given under hba
				hba = list(self.settings.get('hba', []))
				if env.get('vagrant') or hba == True:
					if hba == True:
						hba = []
					with hide('everything'):
						host_ip = run('echo $SSH_CLIENT').split(' ')[0]
					hba.append(('host', 'all', 'all', host_ip + '/32', 'md5'))
				if hba:
					append(_HBA_FILE_PATH, '# Fabric client connections:', use_sudo=True)
				for client in hba:
					client = '%s%s%s%s%s' % (client[0].ljust(8), client[1].ljust(16), client[2].ljust(16), client[3].ljust(24), client[4])
					append(_HBA_FILE_PATH, client, use_sudo=True)
				# Apply any given identity mappings
				ident = self.settings.get('ident', [])
				if ident:
					append(_IDENT_FILE_PATH, '# Fabric username maps:', use_sudo=True)
				for mapping in ident:
					mapping = '%s%s%s' % (mapping[0].ljust(16), mapping[1].ljust(24), mapping[2])
					append(_IDENT_FILE_PATH, mapping, use_sudo=True)
		self.restart()

	def site_install(self, site):
		with hook('site install %s' % self.name, self, site):
			if self.settings.get('postgis'):
				# Install PostGIS also on the site if separate from the server
				if find_service(self.name) is None:
					install_postgis()
			# Install python postgresql support
			apt_get_install('python-dev', 'postgresql-server-dev-all', 'postgresql-client')
			pip_install(site, 'psycopg2')

	def site_config(self, site):
		with hook('site config %s' % self.name, self, site):
			# Create the user for django to access the database with
			if find_service(self.name) is not None:
				with settings(warn_only=True):
					database = get_django_setting(site, 'DATABASES')['default']
					sudo('createuser --createdb --no-superuser --no-createrole %s' % database['USER'], user=POSTGRES_USER)
					sudo("psql -c \"ALTER USER %s WITH PASSWORD '%s';\"" % (database['USER'], database['PASSWORD']), user=POSTGRES_USER)
			# Create the database
			self.execute_sql('CREATE DATABASE %s;' % database['NAME'], site)
			# Setup postgis
			if self.settings.get('postgis'):
				site_config_postgis(self, site)
