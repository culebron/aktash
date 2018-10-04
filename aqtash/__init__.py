import inspect
import argh
import time
from datetime import timedelta
from aqtash import io
from decorator import decorator


def autoargs_once(func):
	"""
	Turns a function into a CLI script, but only when the script is called from shell directly. Otherwise the function remains intact.
	"""
	frm = inspect.stack()[1]
	mod = inspect.getmodule(frm[0])

	#func2 = _args_unpacker(func)
	func2 = func
	func.as_sh = func2


	if mod.__name__ == '__main__':
		# if the script that owns the function is called from shell, then the function should be wrapped
		parser = argh.ArghParser()

		@decorator
		def add_output_file(func, *args, **kwargs):
			# retval = func(*args, **kwargs)
			# if retval is None:
			#	print('Script returned no data, not writing')
			#	return

			args = parser.parse_args()
			output_file = getattr(args, 'output-file')
			io.write_dataframe(io.map(func, *args, **kwargs), output_file)
			# write_file(retval, output_file)

		func3 = add_output_file(func2)

		execution_start = time.time()
		argh.set_default_command(parser, func3)
		parser.add_argument('output-file')
		argh.dispatch(parser)
		print('Total execution time {0}s'.format(timedelta(seconds=time.time()-execution_start)))

	else:
		# otherwise, the wrapped function is needed for an entrypoint script
		mod._autoarg_func = func2
		return func2


def autoargs_nowrite(func):
	"""
	Same as autoargs, but does not require `output_file`.
	"""
	frm = inspect.stack()[1]
	mod = inspect.getmodule(frm[0])
	func2 = _autoargs(func)
	func2.original = func
	
	def func3():
		argh.dispatch_command(func2)

	func2.as_sh = func3
	
	if mod.__name__ == '__main__':
		func3()
	else:
		mod._argh = func3
	return func
