from ..shared import bold, green

_before_hooks = {}
_after_hooks = {}
_hook_depth = 0
_quiet_hooks = False

def before(hook_name):
	def before_decorator(func):
		if not _before_hooks.has_key(hook_name):
			_before_hooks[hook_name] = [func]
		else:
			_before_hooks[hook_name].append(func)
		return func
	return before_decorator

def after(hook_name):
	def after_decorator(func):
		if not _after_hooks.has_key(hook_name):
			_after_hooks[hook_name] = [func]
		else:
			_after_hooks[hook_name].append(func)
		return func
	return after_decorator

class hook(object):
	""" Run registered the hooks. """
	def __init__(self, hook_name, *args, **kwargs):
		self.hook_name = hook_name
		self.args = args
		self.kwargs = kwargs
	def __enter__(self):
		global _hook_depth
		if not _quiet_hooks:
			print bold('%s>Start %s' % ('-' * _hook_depth, self.hook_name))
		if _before_hooks.has_key(self.hook_name):
			for func in _before_hooks[self.hook_name]:
				if func.__doc__:
					print bold(func.__doc__)
				func(*self.args, **self.kwargs)
		_hook_depth += 1
	def __exit__(self, *_):
		global _hook_depth
		_hook_depth -= 1
		if _after_hooks.has_key(self.hook_name):
			for func in _after_hooks[self.hook_name]:
				if func.__doc__:
					print bold(func.__doc__)
				func(*self.args, **self.kwargs)
		if not _quiet_hooks:
			print green('%s>End %s' % ('-' * _hook_depth, self.hook_name))

class quiet_hook_messages(object):
	"""Silence hook output down to one message."""
	def __init__(self, msg):
		print green(msg)
	def __enter__(self):
		global _quiet_hooks
		_quiet_hooks = True
	def __exit__(self, *_):
		global _quiet_hooks
		_quiet_hooks = False
		print bold('Done.')
