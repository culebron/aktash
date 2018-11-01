import inspect
import bdb
import sys
from threading import Thread
import traceback

def thread_map(worker_function, input_array, threads_number=10, **kwargs):
	_inq = []
	_outq = []

	def _worker():
		while len(_inq) > 0:
			input_item = _inq.pop(0)
			try:
				if inspect.isgeneratorfunction(worker_function):
				# добавить inspect и проверять, что функция - генератор
					for output_item in worker_function(input_item, _inq, **kwargs):
						_outq.append(output_item)
				else:
					_outq.append(worker_function(input_item, _inq, **kwargs))
			except Exception as e:
				if isinstance(e, (KeyboardInterrupt, bdb.BdbQuit)):
					raise e
				exc_info = sys.exc_info()
				print('exception in thread, input item:', input_item)
				print('traceback: ')
				traceback.print_exception(*exc_info)
				print('continuing the loop')
				continue

	for item in input_array:
		_inq.append(item)

	if threads_number == 1:
		# single thread - just run syncronously (eg. for inline debugger)
		_worker()

	else:
		threads = []
		for i in range(threads_number):
			thread = Thread(target=_worker)
			thread.start()
			threads.append(thread)

		[i.join() for i in threads]

	return _outq[:]
