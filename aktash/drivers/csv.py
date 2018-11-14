from .abstract import DfDriver, DfReader, DfWriter
from csv import DictReader, field_size_limit
import shapely.wkt, shapely.errors
import pandas as pd
import geopandas as gpd
import io


class CsvReader(DfReader):
	def __init__(self, source, geometry_filter=None, chunk=10000, sep=','):
		super().__init__(source, geometry_filter, chunk)

		if isinstance(self.source, io.BufferedReader):
			self.handler = io.TextIOWrapper(self.source)
		elif isinstance(self.source, str):
			with open(self.source) as f:
				self.total = sum(1 for i in f)

			self.handler = self.source
			
		elif isinstance(self.source, io.TextIOWrapper):
			self.handler = self.source
		
		else:
			raise TypeError(f'source must be either io.TextIOWrapper, or filename string. Got {self.source.__class__} instead.')

		self.reader = pd.read_table(self.handler, chunksize=chunk, sep=sep, engine='c')
	
	def __next__(self):
		df = next(self.reader)
		if 'geometry' not in df:
			return df

		try:
			df['geometry'] = df['geometry'].apply(shapely.wkt.loads)
		except shapely.errors.WKTReadingError:
			# ignore bad WKT (might be not wkt at all)
			return df
		return gpd.GeoDataFrame(df, crs=self.crs)
		

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
