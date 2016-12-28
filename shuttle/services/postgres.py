from importlib import import_module
import StringIO

from fabric.api import run, sudo, cd, put, env, settings, hide
from fabric.context_managers import shell_env
from fabric.contrib.files import append, exists

from .postgis import *
from .service import Service
from ..hooks import hook
from ..shared import apt_get_install, pip_install, find_service, chown

_CONFIG_FILE_PATH = '$(sudo -u postgres psql -t -P format=unaligned -c "SHOW config_file;")'
_HBA_FILE_PATH = '$(sudo -u postgres psql -t -P format=unaligned -c "SHOW hba_file;")'
_IDENT_FILE_PATH = '$(sudo -u postgres psql -t -P format=unaligned -c "SHOW ident_file;")'
_MAIN_DIR = '$(dirname %s)' % _CONFIG_FILE_PATH
_CONF_DIR = '%s/conf.d' % _MAIN_DIR
_POSTGRES_USER = 'postgres'
_POSTGRES_GROUP = 'postgres'
_EXCLUDE_SETTINGS = ['postgis', 'hba', 'ident']

def _pg_quote_config(key, value):
	if (isinstance(value, (str, unicode)) and value not in ('on', 'off') and not value[0].isdigit()) or key == 'listen_addresses':
		return "'%s'" % value
	return value

class Postgres(Service):
	name = 'postgres'
	script = 'postgresql'

	def run_sql(raw_sql, site):
		sql = []
		for line in raw_sql.split('\n'):
			line = line.strip()
			if not line or line.startswith('--'):
				continue
			sql.append(line.replace('\t', '').replace('\n', '').replace("'", "\\'"))
		sql = ' '.join(sql)
		sudo("psql -c $'%s'" % sql)

	def install(self):
		with hook('install %s' % self.name, self):
			apt_get_install('postgresql')
			if self.settings.get('postgis'):
				install_postgis()

	def config(self):
		with hook('config %s' % self.name, self):
			if not exists(_CONF_DIR):
				sudo('mkdir %s' % _CONF_DIR)
				chown(_CONF_DIR, _POSTGRES_USER, _POSTGRES_GROUP)
				append(_CONFIG_FILE_PATH, "include_dir 'conf.d'", use_sudo=True)
			if self.settings:
				# Apply any given settings and place into a new conf.d directory
				config = ''
				for setting in self.settings:
					if setting not in _EXCLUDE_SETTINGS:
						config += '%s = %s\n' % (setting, _pg_quote_config(setting, self.settings[setting]))
				if config:
					chown(put(StringIO.StringIO(config), _CONF_DIR + '/fabric.conf', use_sudo=True, mode=0644), _POSTGRES_USER, _POSTGRES_GROUP)
				# Apply any given Client Authentications given under hba
				hba = list(self.settings.get('hba', []))
				if env.get('vagrant'):
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
				sudo('createdb %s %s' % (connect_args, DATABASES['default']['NAME']), user=user)
				if self.settings.get('postgis'):
					site_config_postgis(connect_args)
