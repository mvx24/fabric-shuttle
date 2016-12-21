import copy
import json
import os
import StringIO
import urllib2
import yaml

from fabric.api import cd, put, settings, sudo
from fabric.context_managers import shell_env
from fabric.contrib.files import exists

from .cron import add_crontab_section
from .service import Service
from ..hooks import hook
from ..shared import apt_get_install, pip_install, red

_PACKAGE_URL = 'http://dl.bintray.com/snowplow/snowplow-generic/snowplow_emr_r77_great_auk.zip'
_CREATE_TABLE_URL = 'https://raw.githubusercontent.com/snowplow/snowplow/master/4-storage/postgres-storage/sql/atomic-def.sql'
_INSTALL_DIR = '/opt/snowplow'
_RUNNER_PATH = os.path.join(_INSTALL_DIR, 'snowplow-emr-etl-runner')
_LOADER_PATH = os.path.join(_INSTALL_DIR, 'snowplow-storage-loader')
_CONFIG_PATH = os.path.join(_INSTALL_DIR, 'config.yml')
_RESOLVER_PATH = os.path.join(_INSTALL_DIR, 'iglu_resolver.json')
_DEFAULT_RESOLVER = {
	"schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
	"data": {
		"cacheSize": 500,
		"repositories": [
			{
				"name": "Iglu Central",
				"priority": 0,
				"vendorPrefixes": [ "com.snowplowanalytics" ],
				"connection": { "http": { "uri": "http://iglucentral.com" } }
			}
		]
	}
}
_CRONTAB_USER = 'root'
_CRONTAB_SECTION = '[snowplow]'
_RUNNER_COMMAND = ' '.join((_RUNNER_PATH,'--config', _CONFIG_PATH, '--resolver', _RESOLVER_PATH, '--skip shred elasticsearch'))
_LOADER_COMMAND = ' '.join((_LOADER_PATH,'--config', _CONFIG_PATH, '--resolver', _RESOLVER_PATH, '--skip shred elasticsearch'))

_DEFAULT_SETTINGS = {
	'runner_schedule': '',
	'loader_schedule': '',
	'shred': False,
	'elasticsearch': False
}

def _config_postgres(target):
	# Assumes that the target database is already installed, running, and setup with the correct credentials but will try to create both the database and table
	response = urllib2.urlopen(_CREATE_TABLE_URL)
	sql = []
	for line in response.readlines():
		line = line.strip()
		if line.startswith('--'):
			continue
		sql.append(line.replace('\t', '').replace('\n', '').replace("'", "\\'"))
	sql = ' '.join(sql)

	# Be sure postgressql client is installed and available
	apt_get_install('postgresql-client')

	pg_env = {
		'PGHOST': target['host'],
		'PGPORT': str(target.get('port', '5432')),
		'PGUSER': target['username'],
		'PGPASSWORD': target['password'],
		'PGDATABASE': target['database']
	}
	with shell_env(**pg_env):
		with settings(warn_only=True):
			# Create the database
			sudo('createdb %s' % target['database'])
			# Create the table - currently only atomic.events is supported as the table name
			if target.get('table', 'atomic.events') != 'atomic.events':
				print red('Only atomic.events is supported as a snowplow postgres storage table name.')
				return
			sudo("psql -c $'%s'" % sql)

class Snowplow(Service):
	name = 'snowplow'
	script = None

	def install(self):
		with hook('install %s' % self.name, self):
			if not exists(_INSTALL_DIR):
				apt_get_install('default-jre', 'unzip')
				sudo('mkdir %s' % _INSTALL_DIR)
				with cd(_INSTALL_DIR):
					sudo('wget --no-clobber %s' % _PACKAGE_URL)
					sudo('unzip %s' % _PACKAGE_URL.split('/')[-1])

	def config(self):
		# Possible configuration options are custom repositories by setting the repositories setting to an array of repository objects
		with hook('config %s' % self.name, self):
			resolver = copy.deepcopy(_DEFAULT_RESOLVER)
			repositories = self.settings.get('repositories')
			if repositories:
				resolver['data']['repositories'].extend(repositories)
			put(StringIO.StringIO(json.dumps(resolver, indent=4)), _RESOLVER_PATH, use_sudo=True, mode=0644)
			put(self.settings['config_file'], _CONFIG_PATH, use_sudo=True, mode=0644)

			# Read the config file for storage configuration
			with open(self.settings['config_file']) as f:
				config = yaml.load(f)
				if config.get('storage') and config['storage'].get('targets'):
					for target in config['storage']['targets']:
						if target.get('type') == 'postgres':
							_config_postgres(target)

			# Schedule cron jobs
			# remove_crontab_section(_CRONTAB_USER, _CRONTAB_SECTION)
			# add_crontab_section(_CRONTAB_USER, _CRONTAB_SECTION, )
