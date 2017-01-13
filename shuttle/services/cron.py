import StringIO

from fabric.api import sudo, put
from fabric.contrib.files import append

from .service import Service
from ..hooks import hook
from ..shared import WWW_USER, get_python_interpreter, get_project_directory, SiteType, chown

class CronSchedule(object):
	def __init__(self, minute='0', hour='0', day_of_month='*', month='*', day_of_week='*'):
		self.minute = str(minute)
		self.hour = str(hour)
		self.day_of_month = str(day_of_month)
		self.month = str(month)
		self.day_of_week = str(day_of_week)

	def __str__(self):
		return ' '.join((self.minute, self.hour, self.day_of_month, self.month, self.day_of_week))

class CronJob(object):
	def __init__(self, command=None, **kwargs):
		self.schedule = CronSchedule()
		self.command = command
		self.management_command = None
		self.directory = None
		self.log_name = None
		self.log_rotate = ('compress', 'daily', 'missingok', 'rotate 30')
		self.chain = None
		self.__dict__.update(kwargs)
		# Remove the schedule from chain because they are on the same
		if self.chain:
			self.chain.schedule = None

	def get_cron_entry(self, site):
		components = []
		if self.schedule:
			components.append(str(self.schedule))
		if self.management_command:
			if site['type'] == SiteType.DJANGO:
				components.append('cd %s &&' % get_project_directory())
				components.append('%s manage.py %s --settings %s' % (get_python_interpreter(site), self.management_command, site['settings_module']))
		elif self.command:
			if self.directory:
				components.append('cd %s &&' % self.directory)
			components.append(self.command)
		if self.log_name:
			components.append('>>/var/log/%s/%s.log 2>&1' % (self.log_name, self.log_name))
		else:
			components.append('>/dev/null 2>&1')
		if self.chain:
			components.append('&&')
			components.append(str(self.chain.get_cron_entry(site)))
		return ' '.join(components)

class ManagementJob(CronJob):
	def __init__(self, management_command, **kwargs):
		kwargs['management_command'] = management_command
		super(ManagementJob, self).__init__(**kwargs)

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
	if isinstance(jobs, CronJob):
		jobs = [jobs]
	for job in jobs:
		lines.append(job.get_cron_entry(site))
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
		# crontab is a single or list of: CronJobs
		# http://www.adminschoice.com/crontab-quick-reference/
		with hook('site config %s' % self.name, self, site):
			section_name = '[fabric] [%s]' % site['name']
			jobs = self.settings.get('crontab')
			if isinstance(jobs, CronJob):
				jobs = [jobs]
			# Remove then add the existing crontab section
			remove_crontab_section(WWW_USER, section_name)
			add_crontab_section(WWW_USER, section_name, jobs, site)
			# Setup logging directories and logrotate.d
			for job in jobs:
				if job.log_name:
					sudo('mkdir -p /var/log/%s' % job.log_name)
					if job.log_rotate:
						rotate_conf = StringIO.StringIO('/var/log/%s/%s.log {\n    %s\n}\n' % (job.log_name, job.log_name, '\n    '.join(job.log_rotate)))
						chown(put(rotate_conf, '/etc/logrotate.d/%s' % job.log_name, use_sudo=True, mode=0644))
