import copy
import json
import os
import StringIO
import yaml

from fabric.api import cd, put, settings, sudo, env
from fabric.context_managers import shell_env
from fabric.contrib.files import exists

from .cron import add_crontab_section, remove_crontab_section, CronSchedule, CronJob
from .postgres import Postgres, POSTGRES_USER
from .service import Service
from ..hooks import hook
from ..shared import apt_get_install, pip_install, red, chown, find_service, SiteType

_PACKAGE_URL = 'http://dl.bintray.com/snowplow/snowplow-generic/snowplow_emr_r77_great_auk.zip'
_MASTER_URL = 'https://codeload.github.com/snowplow/snowplow/zip/master'
_MASTER_FILE = 'snowplow-master.zip'

_INSTALL_DIR = '/opt/snowplow'
_MASTER_DIR = os.path.join(_INSTALL_DIR, 'snowplow-master')
_RUNNER_PATH = os.path.join(_INSTALL_DIR, 'snowplow-emr-etl-runner')
_LOADER_PATH = os.path.join(_INSTALL_DIR, 'snowplow-storage-loader')
_CONFIG_PATH = os.path.join(_INSTALL_DIR, 'config.yml')
_RESOLVER_PATH = os.path.join(_INSTALL_DIR, 'iglu_resolver.json')
_CREATE_TABLE_PATH = os.path.join(_MASTER_DIR, '4-storage/postgres-storage/sql/atomic-def.sql')
_ENRICHMENTS_PATH = os.path.join(_MASTER_DIR, '3-enrich/config/enrichments')
_DEFAULT_RESOLVER = {
	'schema': 'iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0',
	'data': {
		'cacheSize': 500,
		'repositories': [
			{
				'name': 'Iglu Central',
				'priority': 0,
				'vendorPrefixes': [ 'com.snowplowanalytics' ],
				'connection': { 'http': { 'uri': 'http://iglucentral.com' } }
			}
		]
	}
}
_CRONTAB_USER = 'root'
_CRONTAB_SECTION = '[snowplow]'
_RUNNER_COMMAND = ' '.join((_RUNNER_PATH, '--config', _CONFIG_PATH, '--resolver', _RESOLVER_PATH))
_LOADER_COMMAND = ' '.join((_LOADER_PATH, '--config', _CONFIG_PATH))

_DEFAULT_SETTINGS = {
	'schedule': CronSchedule(45, 2),
	'chain': None,
	'enrichments': _ENRICHMENTS_PATH,
	'runner_skip': (),
	'loader_skip': ()
}

def _config_postgres(target):
	# Assumes that the target database is already installed, running, and setup with the correct credentials but will try to create both the database and table
	apt_get_install('postgresql-client')
	pg_env = {
		'PGHOST': target['host'],
		'PGPORT': str(target.get('port', '5432')),
		'PGUSER': target['username'],
		'PGPASSWORD': target['password'],
		'PGDATABASE': target['database']
	}
	# Create the user if a local database
	if find_service(Postgres.name) is not None:
		with settings(warn_only=True):
			sudo('createuser --echo --createdb --no-superuser --no-createrole %s' % target['username'], user=POSTGRES_USER)
			sudo("psql --echo-queries -c \"ALTER USER %s WITH PASSWORD '%s';\"" % (target['username'], target['password']), user=POSTGRES_USER)
	with shell_env(**pg_env):
		with settings(warn_only=True):
			# Create the database
			sudo('createdb --echo %s' % target['database'])
			# Create the table - currently only atomic.events is supported as the table name
			if target.get('table', 'atomic.events') != 'atomic.events':
				print red('Only atomic.events is supported as a snowplow postgres storage table name.')
				return
			sudo('psql -f %s' % _CREATE_TABLE_PATH)

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
					sudo('wget --no-clobber --output-document %s %s' % (_MASTER_FILE, _MASTER_URL))
					sudo('unzip %s' % _MASTER_FILE)

	def config(self):
		# Possible configuration options are custom repositories by setting the repositories setting to an array of repository objects
		with hook('config %s' % self.name, self):
			resolver = copy.deepcopy(_DEFAULT_RESOLVER)
			repositories = self.settings.get('repositories')
			if repositories:
				resolver['data']['repositories'].extend(repositories)
			chown(put(StringIO.StringIO(json.dumps(resolver, indent=4)), _RESOLVER_PATH, use_sudo=True, mode=0644))
			chown(put(self.settings['config_file'], _CONFIG_PATH, use_sudo=True, mode=0644))

			# Read the config file for storage configuration
			with open(self.settings['config_file']) as f:
				config = yaml.load(f)
				if config.get('storage') and config['storage'].get('targets'):
					for target in config['storage']['targets']:
						if target.get('type') == 'postgres':
							_config_postgres(target)

			# Schedule cron jobs
			chain = self.settings.get('chain', _DEFAULT_SETTINGS['chain'])
			if not chain.log_name:
				chain.log_name = self.name
			loader_skip = ','.join(self.settings.get('loader_skip', _DEFAULT_SETTINGS['loader_skip']))
			if loader_skip:
				loader_skip = ' --skip ' + loader_skip
			loader_job = CronJob(_LOADER_COMMAND + loader_skip, log_name=self.name, chain=chain)
			runner_skip = ','.join(self.settings.get('runner_skip', _DEFAULT_SETTINGS['runner_skip']))
			if runner_skip:
				runner_skip = ' --skip ' + runner_skip
			runner_enrichments = self.settings.get('enrichments', _DEFAULT_SETTINGS['enrichments'])
			if runner_enrichments:
				runner_enrichments = ' --enrichments ' + runner_enrichments
			runner_job = CronJob(_RUNNER_COMMAND + runner_enrichments + runner_skip, log_name=self.name, schedule=self.settings.get('schedule', _DEFAULT_SETTINGS['schedule']), chain=loader_job)

			# Configure cron with the first site with the Snowplow service or just the active or first site if no Snowplow service is found
			sites = env['sites'].values() or []
			snowplow_site = env.get('site') or sites[0]
			for site in sites:
				if site:
					found = False
					for service in site.get('services', []):
						if isinstance(service, Snowplow):
							found = True
							break
					if found:
						snowplow_site = site
						break
			# Because of using a site before siteinstall setup virtualenv here for proper Django management commands
			if snowplow_site['type'] == SiteType.DJANGO:
				apt_get_install('python-pip')
				pip_install(None, 'virtualenv')
			remove_crontab_section(_CRONTAB_USER, _CRONTAB_SECTION)
			add_crontab_section(_CRONTAB_USER, _CRONTAB_SECTION, runner_job, snowplow_site)
