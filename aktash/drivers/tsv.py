from .csv import CsvReader, CsvWriter, CsvDriver
from csv import DictReader
import shapely.wkt, shapely.errors
import io


class TsvReader(CsvReader):
	def __init__(self, source, geometry_filter=None, chunk=10000):
		super().__init__(source, geometry_filter, chunk)

		if isinstance(self.source, io.BufferedReader):
			self.handler = io.TextIOWrapper(self.source)
		elif isinstance(self.source, str):
			with open(self.source) as f:
				self.total = sum(1 for i in f)
			self.handler = open(self.source, sep='\t')
		elif isinstance(self.source, io.TextIOWrapper):
			self.handler = self.source
		else:
			raise TypeError(f'source must be either io.TextIOWrapper, or filename string. Got {self.source.__class__} instead.')


		self.reader = DictReader(self.handler)
		self.fieldnames = self.reader.fieldnames
	

class TsvWriter(CsvWriter):
	def writedf(self, df):
		from csv import DictWriter
		if self.handler is None:
			self.fieldnames = list(df)

			if isinstance(self.target, str):
				self._cleanup_target()
				self.handler = open(self.target, 'w', sep='\t')
			elif isinstance(self.target, io.TextIOWrapper):
				self.handler = self.target

			self.writer = DictWriter(self.handler, fieldnames=self.fieldnames, extrasaction='ignore')
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
