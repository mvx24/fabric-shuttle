from setuptools import setup, find_packages

setup(
	name='fabric-shuttle',
	version='0.0.17',
	description='Declarative service-oriented approach with hooks for using Fabric to provision a machine or vagrant box.',
	url='https://github.com/mvx24/fabric-shuttle',
	author='mvx24',
	author_email='cram2400@gmail.com',
	license='MIT',
	classifiers=[
		'Development Status :: 4 - Beta',
		'Environment :: Console',
		'Framework :: Django',
		'License :: OSI Approved :: MIT License',
		'Operating System :: MacOS :: MacOS X',
		'Operating System :: POSIX',
		'Operating System :: Unix',
		'Programming Language :: Python :: 2.7',
		'Programming Language :: Python :: 2 :: Only',
		'Topic :: System :: Installation/Setup',
		'Topic :: System :: Software Distribution',
		'Topic :: System :: Systems Administration'
	],
	keywords='fabric deployment development vagrant provisioning services',
	packages=find_packages(),
	install_requires=['fabric', 'boto', 'django', 'pyyaml'],
	package_data={
		'': ['templates/*']
	}
)
