#!%(interpreter)s
import os
import sys

if __name__ == '__main__':
	# NOTE: os.chdir() to the project does not work for loading up the settings module so instead add to sys.path
	# os.chdir is included however so that usage of os.path.abspath('.') will resolve correct
	os.chdir('%(project_dir)s')
	sys.path.append('%(project_dir)s')
	os.environ.setdefault('DJANGO_SETTINGS_MODULE', '%(settings_module)s')
	from django.core.management import execute_from_command_line
	execute_from_command_line(sys.argv)
