from functools import wraps
import inspect
from gistalt import autoargs

def testdec(func):
	gen_func = func
	if not inspect.isgeneratorfunction(func):
		@wraps(func)
		def gen_func(prev_df, next_df):
			yield func(prev_df, next_df)

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

@testdec
def reducer(prev_df, next_df):
	prev_df = prev_df or 0
	if next_df % 2 == 1:
		return None, prev_df + next_df
	return prev_df + next_df


items = [1, 2, 3, 4, 5, None, 6, None, 7, 8, 16]

@autoargs
def main():
	for i in reducer(items):
		print(i)
