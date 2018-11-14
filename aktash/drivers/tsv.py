from .csv import CsvReader, CsvWriter, CsvDriver
from csv import DictReader
import io
import pandas as pd
import shapely.wkt, shapely.errors


class TsvReader(CsvReader):
	def __init__(self, source, geometry_filter=None, chunk=10000, sep='\t'):
		super().__init__(source, geometry_filter, chunk, sep)
			

class TsvWriter(CsvWriter):
	def writedf(self, df):
		from csv import DictWriter
		if self.handler is None:
			self.fieldnames = list(df)

			if isinstance(self.target, str):
				self._cleanup_target()
				self.handler = open(self.target, 'w')
			elif isinstance(self.target, io.TextIOWrapper):
				self.handler = self.target

			self.writer = DictWriter(self.handler, fieldnames=self.fieldnames, extrasaction='ignore', delimiter='\t')
			self.writer.writeheader()

		if 'geometry' in df:
			df['geometry'] = df['geometry'].apply(shapely.wkt.dumps)
			
		self.writer.writerows(df.to_dict(orient='records'))
		self.handler.flush()


class TsvDriver(CsvDriver):
	source_extension = 'tsv'
	reader = TsvReader
	writer = TsvWriter


driver = TsvDriver
