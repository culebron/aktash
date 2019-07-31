from concurrent.futures import ThreadPoolExecutor, as_completed
from aktash import AKDBG
from threading import Thread
from tqdm import tqdm
import bdb
import inspect
import sys
import traceback

def threader(worker_function, input_array, threads_number=10):
	_inq = []
	_outq = []

	def _worker():
		while len(_inq) > 0:
			input_item = _inq.pop(0)
			try:
				if inspect.isgeneratorfunction(worker_function):
				# добавить inspect и проверять, что функция - генератор
					for output_item in worker_function(input_item, _inq):
						_outq.append(output_item)
				else:
					_outq.append(worker_function(input_item, _inq))
			except Exception as e:
				if isinstance(e, (KeyboardInterrupt, bdb.BdbQuit)) or AKDBG:
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


def pooler(worker_function, input_array, threads=10, tqdm_=None, errors=None):
	if threads < 2:
		for data in input_array:
			yield worker_function(*data)
		return

	if tqdm_:
		try:
			l = len(input_array)
		except:
			l = None
		t = tqdm(total=l, desc=f'routing (with geometries) {threads} threads')

	with ThreadPoolExecutor(max_workers=threads) as e:
		future_map = {e.submit(worker_function, data): data for data in input_array}
		for future in as_completed(future_map):
			if future.exception() is None:
				yield future.result(), future_map[future]
			elif errors == 'ignore':
				yield None, future_map[future]
			else:
				raise future.exception()
			if tqdm_:
				t.update()

