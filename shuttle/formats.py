import StringIO

def format_ini(values, quotes=True):
	output = StringIO.StringIO()
	for section in values:
		print >>output, '[%s]' % section
		for key, value in values[section].iteritems():
			value = str(value)
			if quotes and (';' in value or '=' in value or '#' in value or ' ' in value):
				value = '"%s"' % value
			print >>output, '%s=%s' % (key, value)
		print ''
	value = output.getvalue()
	output.close()
	return value
