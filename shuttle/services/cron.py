from fabric.api import sudo, env
from fabric.contrib.files import append

from .nginx import NGINX_USER
from .service import Service
from ..hooks import hook
from ..shared import get_python_interpreter, get_project_directory, SiteType

def read_crontab(user):
	result = sudo('crontab -u %s -l' % user, warn_only=True)
	return result.splitlines() if result.succeeded else []

def write_crontab(user, lines):
	if lines:
		import sha
		lines = '\n'.join(lines)
		crontab_file = '/tmp/fabric_crontab_' + sha.new(lines).digest().encode('hex')[0:10]
		sudo('touch ' + crontab_file)
		append(crontab_file, lines, use_sudo=True)
		sudo('crontab -u %s %s' % (user, crontab_file))
		sudo('rm ' + crontab_file)
	else:
		sudo('crontab -u %s -r' % user, warn_only=True)

def add_crontab_section(user, section_name, jobs, site):
	if not jobs:
		return
	lines = read_crontab(user)
	start = '# start %s' % section_name
	end = '# end %s' % section_name
	lines.append(start)
	if isinstance(jobs, (str, unicode)):
		jobs = [jobs]
	for job in jobs:
		if isinstance(job, (str, unicode)):
			if site and site['type'] == SiteType.DJANGO:
				lines.append('0 0 * * * cd %s && %s manage.py %s --settings %s >/dev/null 2>&1' % (get_project_directory(), get_python_interpreter(site), job, site['settings_module']))
			else:
				lines.append(job)
		else:
			lines.append(' '.join(job))
	lines.append(end)
	write_crontab(user, lines)

def remove_crontab_section(user, section_name):
	lines = read_crontab(user)
	start = '# start %s' % section_name
	end = '# end %s' % section_name
	try:
		# Remove the previous section
		start_index = lines.index(start)
		end_index = lines.index(end)
		lines = lines[0:start_index] + lines[end_index+1:]
	except:
		pass
	write_crontab(user, lines)

class Cron(Service):
	name = 'cron'
	script = None

	def site_config(self, site):
		# Creates a new crontab for the nginx user.
		# list of (5,6,'*',0,'full shell command') or just 'management command'
		# http://www.adminschoice.com/crontab-quick-reference/
		with hook('site config %s' % self.name, self, site):
			section_name = '[fabric] [%s]' % site['name']
			jobs = self.settings.get('crontab')
			# Remove then add the existing crontab section
			remove_crontab_section(NGINX_USER, section_name)
			add_crontab_section(NGINX_USER, section_name, jobs, site)
