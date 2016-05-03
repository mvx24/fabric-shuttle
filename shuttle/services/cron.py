from fabric.api import sudo, env
from fabric.contrib.files import append

from .nginx import NGINX_USER
from .service import Service
from ..hooks import hook

class Cron(Service):
	name = 'cron'
	script = None

	def site_config(self, site):
		# Creates a new crontab for the nginx user.
		# list of (5,6,'*',0,'full shell command') or just 'management command'
		# http://www.adminschoice.com/crontab-quick-reference/
		with hook('site config %s' % self.name, self, site):
			# Get the existing crontab
			result = sudo('crontab -l -u %s' % NGINX_USER, warn_only=True)
			lines = result.splitlines() if result.succeeded else []
			start = '# start [fabric] [%s]' % site['name']
			end = '# end [fabric] [%s]' % site['name']
			try:
				# Remove the previous section
				start_index = lines.index(start)
				end_index = lines.index(end)
				lines = lines[0:start_index] + lines[end_index+1:]
			except:
				pass
			if self.settings.get('crontab'):
				lines.append(start)
				for job in self.settings['crontab']:
					if isinstance(job, (str, unicode)):
						lines.append('0 0 * * * cd %s && python manage.py %s --settings %s >/dev/null 2>&1' % ('/srv/www/%s' % env['project'], job, site['settings_module']))
					else:
						lines.append(' '.join())
				lines.append(end)
			if lines:
				import sha
				lines = '\n'.join(lines)
				crontab_file = '/tmp/fabric_crontab_' + sha.new(lines).digest().encode('hex')[0:10]
				sudo('touch ' + crontab_file)
				append(crontab_file, lines, use_sudo=True)
				sudo('crontab -u %s %s' % (NGINX_USER, crontab_file))
				sudo('rm ' + crontab_file)
			else:
				sudo('crontab -r %s' % NGINX_USER, warn_only=True)
