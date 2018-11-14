from .abstract import DfDriver, DfReader, DfWriter
from csv import DictReader, field_size_limit
import shapely.wkt, shapely.errors
import pandas as pd
import geopandas as gpd
import io


class CsvReader(DfReader):
	def __init__(self, source, geometry_filter=None, chunk=10000):
		super().__init__(source, geometry_filter, chunk)

		if isinstance(self.source, io.BufferedReader):
			self.handler = io.TextIOWrapper(self.source)
		elif isinstance(self.source, str):
			with open(self.source) as f:
				self.total = sum(1 for i in f)
			self.handler = open(self.source)
		elif isinstance(self.source, io.TextIOWrapper):
			self.handler = self.source
		else:
			raise TypeError(f'source must be either io.TextIOWrapper, or filename string. Got {self.source.__class__} instead.')


		self.reader = DictReader(self.handler)
		self.fieldnames = self.reader.fieldnames
	
	def _make_df(self, rows):
		if 'geometry' in self.fieldnames:
			return gpd.GeoDataFrame(rows, crs=self.crs)
		else:
			return pd.DataFrame(rows)

	def _next_row_data(self):
		if self.handler.closed:
			raise StopIteration

		data = dict(next(self.reader))
		if 'geometry' in data:
			try:
				data['geometry'] = shapely.wkt.loads(data['geometry'])
			except shapely.errors.WKTReadingError:
				# ignore bad WKT (might be not wkt at all)
				return data

		return data


class CsvWriter(DfWriter):
	def __init__(self, target):
		super().__init__(target)
		field_size_limit(10000000)

	def writedf(self, df):
		from csv import DictWriter
		if self.handler is None:
			self.fieldnames = list(df)

			if isinstance(self.target, str):
				self._cleanup_target()
				self.handler = open(self.target, 'w')
			elif isinstance(self.target, io.TextIOWrapper):
				self.handler = self.target

			self.writer = DictWriter(self.handler, fieldnames=self.fieldnames, extrasaction='ignore')
			self.writer.writeheader()

		if 'geometry' in df:
			df['geometry'] = df['geometry'].apply(shapely.wkt.dumps)
			
		self.writer.writerows(df.to_dict(orient='records'))
		self.handler.flush()


class CsvDriver(DfDriver):
	source_extension = 'csv'
	reader = CsvReader
	writer = CsvWriter


driver = CsvDriver
