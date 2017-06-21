#!%(interpreter)s
import os
import sys

if __name__ == '__main__':
	# NOTE: some commands like runserver will start a new process based on the management script in argv[0]
	# force it here to be absolute because the new process will be in the project directory
	sys.argv[0] = os.path.abspath(__file__)
	# NOTE: os.chdir() to the project does not work for loading up the settings module so instead add to sys.path
	# os.chdir is included however so that usage of os.path.abspath('.') will resolve correctly within the project
	os.chdir('%(project_dir)s')
	sys.path.append('%(project_dir)s')
	os.environ.setdefault('DJANGO_SETTINGS_MODULE', '%(settings_module)s')
	from django.core.management import execute_from_command_line
	execute_from_command_line(sys.argv)
