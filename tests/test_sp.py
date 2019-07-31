from gistalt.op.buffer import process as buffer
from gistalt import io, autoargs
#from time import sleep

@autoargs
def main(input_path, output_path):
	def fn(df):
		return buffer(df, 50)

	def fn2(df):
		return buffer(df, 100)

	io.write_dataframe(io.map(io.map(input_path, fn), fn2), output_path)
	
