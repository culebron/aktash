from time import sleep
from gistalt import autoargs
from gistalt.op.buffer import process as buffer
from contextlib import ExitStack
from aktash import mr, AKDEBUG

@autoargs
def main(input_path, output_path):
	# @mr._map_dec
	def fn(df):
		return buffer(df, 50)

	def fn2(df):
		return buffer(df, 100)

	with ExitStack() as stack:
		if AKDEBUG:
			import ipdb
			stack.enter_context(ipdb.slaunch_ipdb_on_exception())

		# for df in mr.DfStream(input_path):
		# 	fn()
		
		mr.DfStream(input_path, workers=6).map(fn, fn2).write(output_path)
