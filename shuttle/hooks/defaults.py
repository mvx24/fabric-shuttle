from fabric.api import settings, sudo
from fabric.contrib.files import exists

from .decorators import before, after
from ..shared import apt_get_install

@before('pip install paramiko')
def install_paramiko_libs():
	apt_get_install('build-essential', 'libssl-dev', 'libffi-dev')

@before('pip install pil')
def install_img_libs():
	apt_get_install('libjpeg-dev', 'libpng-dev', 'libfreetype6-dev')

@before('pip install psycopg2')
def install_psycopg2_libs():
	apt_get_install('python-dev', 'libpq-dev', 'postgresql-client')

# Setup image libraries properly
# http://stackoverflow.com/questions/7648200/pip-install-pil-e-tickets-1-no-jpeg-png-support

@after('apt-get install libjpeg-dev')
def setup_libjpeg():
	with settings(warn_only=True):
		if exists('/usr/lib/x86_64-linux-gnu/libjpeg.so'):
			sudo('ln -sf /usr/lib/x86_64-linux-gnu/libjpeg.so /usr/lib/libjpeg.so')
		if exists('/usr/lib/x86_64-linux-gnu/libz.so'):
			sudo('ln -sf /usr/lib/x86_64-linux-gnu/libz.so /usr/lib/libz.so')

@after('apt-get install libfreetype6-dev')
def setup_libfreetype6():
	with settings(warn_only=True):
		if exists('/usr/lib/x86_64-linux-gnu/libfreetype.so'):
			sudo('ln -sf /usr/lib/x86_64-linux-gnu/libfreetype.so /usr/lib/libfreetype.so')
		if exists('/usr/lib/x86_64-linux-gnu/libz.so'):
			sudo('ln -sf /usr/lib/x86_64-linux-gnu/libz.so /usr/lib/libz.so')
