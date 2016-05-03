from importlib import import_module
import os

from service import Service
from ..hooks import hook
from ..shared import bold, red

def get_aws_access_key(site):
	if site.has_key('webapp') and site['webapp'].get('aws_access_key_id') and site['webapp'].get('aws_secret_access_key'):
		return site['webapp']['aws_access_key_id'], site['webapp']['aws_secret_access_key']
	else:
		try:
			module = import_module(site['settings_module'])
			return module.AWS_ACCESS_KEY_ID, module.AWS_SECRET_ACCESS_KEY
		except:
			print red('Error: Could not access AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY from settings.')
			exit(1)

def setup_aws_access_key(site):
	# Boto variables
	aws_access_key_id, aws_secret_access_key = get_aws_access_key(site)
	os.environ.setdefault('AWS_ACCESS_KEY_ID', aws_access_key_id)
	os.environ.setdefault('AWS_SECRET_ACCESS_KEY', aws_secret_access_key)

def upload_to_s3(site, bucket, directory=None, files=None, prefix=None):
	"""Uploads files to an s3 bucket. Upload either an entire directory with files=None, or specific files with and optional directory prefix."""
	if bucket is None:
		print red('Error: Bucket must be specified.')
		return
	if directory is None and files is None:
		print red('Error: Directory and/or files must be specified.')
		return
	# Setup boto
	import boto
	from boto.s3.bucket import Bucket
	from boto.s3.key import Key
	import mimetypes
	import fnmatch

	setup_aws_access_key(site)

	# Connect to S3
	c = boto.connect_s3()
	b = Bucket(c, bucket)

	# Fix the prefix
	# prefix itself shouldn't have a / prefix itself but should end with /
	if prefix:
		prefix = prefix.lstrip('/')
		if prefix and not prefix.endswith('/'):
			prefix = prefix + '/'

	def __upload(key, filename):
		k = Key(b)
		k.key = key
		headers = {}
		content_type = mimetypes.guess_type(filename)[0]
		if site.has_key('webapp') and site['webapp'].get('cache_control'):
			for pattern in site['webapp']['cache_control']:
				if fnmatch.fnmatch(filename, pattern):
					headers['Cache-Control'] = site['webapp']['cache_control'][pattern]
					break
		if site.has_key('webapp') and site['webapp'].get('gzip_types') and content_type in site['webapp']['gzip_types']:
			from gzip import GzipFile
			from StringIO import StringIO
			# Need to specify content_type when uploading from a string!
			headers['Content-Type'] = content_type
			headers['Content-Encoding'] = 'gzip'
			s = StringIO()
			g = GzipFile(fileobj=s, mode='wb')
			with open(filename, 'rb') as f:
				g.write(f.read())
			g.close()
			k.set_contents_from_string(s.getvalue(), headers)
		else:
			k.set_contents_from_filename(filename, headers)

	if files:
		# Upload individual files
		if directory:
			keys = [filename.lstrip('/') for filename in files]
			files = [os.path.join(directory, filename) for filename in files]
		else:
			keys = [os.path.split(filename)[1] for filename in files]
		for i, filename in enumerate(files):
			print 'Uploading %s' % keys[i]
			if prefix:
				key = prefix + keys[i]
			else:
				key = keys[i]
			__upload(key, filename)
	elif directory:
		# Upload an entire directory
		def __upload_dir(arg, dirname, names):
			# arg is the starting directory
			for name in names:
				filename = os.path.join(dirname, name)
				if not os.path.isdir(filename) and not os.path.islink(filename) and not name.startswith('.'):
					key = filename[len(arg):]
					if key.startswith('/'):
						key = key[1:]
					if prefix:
						key = prefix + key
					print 'Uploading %s' % key
					__upload(key, filename)
		os.path.walk(directory, __upload_dir, directory)

def delete_from_s3(site, bucket, prefix=None):
	""" Remove all files with the prefix specified from the bucket. """
	if bucket is None:
		print red('Error: Bucket must be specified.')
		return
	# Setup boto
	import boto
	from boto.s3.bucket import Bucket
	from boto.s3.key import Key

	setup_aws_access_key(site)

	# Fix the prefix
	if prefix:
		prefix = prefix.lstrip('/')

	# Connect to S3, list the contents, and remove all of the keys
	c = boto.connect_s3()
	b = Bucket(c, bucket)
	result_set = b.list(prefix=prefix)
	result = b.delete_keys([key.name for key in result_set])

def create_public_bucket_policy(bucket_name):
	import json
	policy = {
		"Version": "2008-10-17",
		"Statement": [
			{
				"Sid": "AddPerm",
				"Effect": "Allow",
				"Principal": {
					"AWS": "*"
				},
				"Action": "s3:GetObject",
				"Resource": "arn:aws:s3:::%s/*" % bucket_name
			}
		]
	}
	return json.dumps(policy)

DEFAULT_CORS_RULE = {'allowed_method': ['GET'], 'allowed_origin': ['*'], 'allowed_header': ['Authorization'], 'max_age_seconds': 3000}

class S3(Service):
	name = 's3'
	script = None

	def site_config(self, site):
		with hook('site config %s' % self.name, self, site):
			setup_aws_access_key(site)

			from boto import connect_s3
			from boto.s3.bucket import Bucket
			from boto.s3.key import Key

			for bucket_config in self.settings['buckets']:
				# Connect and make sure the bucket exists
				print bold(u'Configuring bucket %s...' % bucket_config['name'])
				connection = connect_s3()
				try:
					bucket = connection.get_bucket(bucket_config['name'])
				except:
					bucket = connection.create_bucket(bucket_config['name'])
				# Set the bucket policy
				if bucket_config.has_key('policy'):
					bucket.set_policy(bucket_config['policy'])
				# Setup CORS, array of rules
				# http://boto.readthedocs.org/en/latest/ref/s3.html#boto.s3.cors.CORSConfiguration
				if bucket_config.has_key('cors') and bucket_config['cors'] is None:
					# If explicity set to None, then remove the cors policy
					bucket.delete_cors()
				else:
					if not bucket_config.has_key('cors'):
						# If not specified, use the default GET policy
						bucket_config['cors'] = (DEFAULT_CORS_RULE,)
					from boto.s3.cors import CORSConfiguration
					cors_config = CORSConfiguration()
					for rule in bucket_config['cors']:
						cors_config.add_rule(**rule)
					bucket.set_cors(cors_config)
				# Setup the lifecycle, array of rules
				# http://boto.readthedocs.org/en/latest/ref/s3.html#boto.s3.lifecycle.Lifecycle
				if bucket_config.has_key('lifecycle'):
					from boto.s3.lifecycle import Lifecycle
					lifecycle_config = Lifecycle()
					for rule in bucket_config['lifecycle']:
						lifecycle_config.add_rule(**rule)
					bucket.configure_lifecycle(lifecycle_config)
				else:
					bucket.delete_lifecycle_configuration()
				# Setup the bucket website hosting {suffix, error_key, routing_rules, redirect_all_requests_to}
				# http://boto.readthedocs.org/en/latest/ref/s3.html
				# https://github.com/boto/boto/blob/develop/boto/s3/website.py
				if bucket_config.has_key('website'):
					# Expand the routing rules, array of {condition, redirect}
					if bucket_config['website'].has_key('routing_rules'):
						from boto.s3.website import RoutingRules, RoutingRule
						routing_rules = RoutingRules()
						for rule in bucket_config['website']['routing_rules']:
							routing_rules.add_rule(RoutingRule(**rule))
						bucket_config['website']['routing_rules'] = routing_rules
					# Expand the redirect, redirect_all_requests_to is {hostname, protocol}
					if bucket_config['website'].has_key('redirect_all_requests_to'):
						from boto.s3.website import RedirectLocation
						bucket_config['website']['redirect_all_requests_to'] = RedirectLocation(**bucket_config['website']['redirect_all_requests_to'])
					bucket.configure_website(**bucket_config['website'])
				else:
					bucket.delete_website_configuration()
