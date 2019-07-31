#!/usr/bin/python3

# mr (map/reduce)
from functools import wraps
from multiprocessing import cpu_count, Queue, Process
import inspect
from . import io, AKDEBUG
import sys


def _map_dec(func):
	@wraps(func)
	def decorated(*args, **kwargs):
		gen = func(*args, **kwargs)
		if not inspect.isgenerator(gen):
			gen = [gen]
		for val in gen:
			if val is not None:
				yield val

	return decorated

def _reduce_dec(func):
	gen_func = func
	if not inspect.isgeneratorfunction(func):
		@wraps(func)
		def gen_func(prev_df, next_df):
			yield func(prev_df, next_df)

	@wraps(gen_func)
	def decorated(source):
		store = [None]
		for next_df in source:
			if next_df is None:
				continue

			prev_df = store[0]
			for data in gen_func(prev_df, next_df):
				if not isinstance(data, (tuple, list)):
					data = (data,)

				if len(data) == 0:
					raise ValueError(f'{func.__name__} outputs a 0-length list')

				if len(data) > 2:
					raise ValueError(f'{func.__name__} outputs move than 2 items, can\'t unpack')

				if len(data) == 1:
					store[0] = data[0]

				else:
					store[0], emit = data
					yield emit

		yield store[0]

	return decorated

def debug_print(*args, **kwargs):
	if AKDEBUG:
		with open('/tmp/debug.txt', 'a') as f:
			print(*args, **kwargs, file=f)
		sys.stdout.flush()

class DfStream:
	def __init__(self, source, qlength=None, workers=None):
		self.output_none_limit = self.workers = workers or (cpu_count() - 2)
		qlength = qlength or self.workers
		self._read_process = None
		self._work_processes = []
		self.worker_functions = []

		# gen is a generator or a gen func
		if hasattr(source, 'output_q') and hasattr(source, 'err_q'):
			self.gen = source
			self.input_q = source.output_q
			self.err_q = source.err_q
		else:
			self.gen = io.stream_reader(source)
			self.input_q = Queue(maxsize=qlength) # reader will put here
			self.output_q = Queue(maxsize=qlength) # processors will put here  # make this work if no processors are here
			self.err_q = Queue(maxsize=qlength)

	def _read_routine(self):
		debug_print('!!! reading routine started')
		
		self._gen = self.gen() if inspect.isgeneratorfunction(self.gen) else self.gen
		iterator = iter(self._gen)
		while True:
			debug_print('!!! waiting input')
			# if there's an error, and we're not in debug mode, stop everything
			if not self.err_q.empty() and not AKDEBUG:
				break

			try:
				df = next(iterator)
			except StopIteration:
				debug_print('reader ended')
				break
			except Exception as e:
				debug_print('reader error', e)
				self.err_q.put(e)
				break
			else:
				debug_print('reader ok', len(df))
				self.input_q.put(df)

		debug_print('end reading')
		self.input_q.put(None)

	def _work_step(self, item, funcs):
		"""
		Since there is a chain of workers, each of them is wrapped in this method.
		It calles the worker and then parses its output and either calls the next one
		or yields the data.
		"""
		next_func = funcs[0]
		gen = next_func(item)
		if len(funcs) > 1:
			for item2 in gen:
				for item3 in self._work_step(item2, funcs[1:]):
					yield item2
		else:
			for item2 in gen:
				yield item2

	def _work_routine(self):
		while True:
			if not self.err_q.empty():
				break # error, quit

			df = self.input_q.get()
			if df is None: # stop signal
				self.output_q.put(None)
				self.input_q.put(None)
				debug_print('ending worker process')
				break

			try:
				for item in self._work_step(df, self.worker_functions):
					self.output_q.put(item) # _process_step already removes None items, no need to check
			except Exception as e:
				if AKDEBUG:
					print(e)

				self.err_q.put(e)	
				self.input_q.put(None)
				self.output_q.put(None)
				raise e

	def __iter__(self):
		debug_print('iterating')
		self._read_process = Process(None, self._read_routine, args=())
		debug_print('starting reader')
		self._read_process.start()
		
		if len(self.worker_functions) == 0: # input_q is forwarded directly to output_q
			debug_print('no processors')
			self.output_q = self.input_q
			self.output_none_limit = 1
		else:
			self._work_processes = [Process(None, self._work_routine) for i in range(self.workers)]
			for pr in self._work_processes:
				pr.start()

		return self

	def __next__(self):
		print('waiting for output')

		data = self.output_q.get()
		if data is None: # end of pipeline
			raise StopIteration
		return data

	def map(self, *funcs):
		self.worker_functions.extend([_map_dec(f) for f in funcs])
		return self

	def reduce(self, *funcs):
		self.worker_functions.extend([_reduce_dec(f) for f in funcs])
		return self

	def _write_routine(self, target):
		debug_print('started _write_routine')
		self.writer = io.stream_writer(target)
		with self.writer as write:
			while True:
				if not self.err_q.empty():
					debug_print('writer: error')
					e = self.err_q.get()
					[i.terminate() for i in self._work_processes]
					self._read_process.terminate()
					print(e)
					raise e
						
				debug_print('writer: reading from q')
				df = self.output_q.get()
				if df is None:
					debug_print('writer: got NONE')
					self.output_none_limit -= 1 # one more process ended
					if self.output_none_limit == 0:
						break

				write(df)

	def write(self, target):
		iter(self)
		self._write_process = Process(None, self._write_routine, args=(target,))
		self._write_process.start()
		debug_print('writer working')
		self._write_process.join()
