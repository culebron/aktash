#!/usr/bin/python3.6

from .abstract import DfDriver, DfReader, DfWriter
from csv import field_size_limit
from shapely.wkt import loads
import shapely.errors
import pandas as pd
import geopandas as gpd
import io
import os


class CsvReader(DfReader):
	def __init__(self, source, geometry_filter=None, chunk_size=10000, skip=0, sep=','):
		if not os.path.exists(source):  # immediately raise error to avoid crashing much later
			raise FileNotFoundError(f'file {source} does not exist')

		self.sep = sep  # needed in _read_schema
		super().__init__(source, geometry_filter, chunk_size, skip)
		self.reader = None

	def _read_schema(self):
		# reading schema, should be like fiona schema
		df = pd.read_csv(self.source, nrows=1, sep=self.sep, engine='c')
		self.fieldnames = list(df)
		properties = {k: df[k].dtype for k in self.fieldnames}
		self.schema = {'properties': properties}

		geom_col = None
		if 'geometry' in properties:
			geom_col = 'geometry'
		elif 'WKT' in properties:
			geom_col = 'WKT'

		if geom_col:
			properties.pop(geom_col)
			self.schema['geometry'] = loads(df[geom_col][0]).geom_type

		with open(self.source) as f:
			self.total = sum(1 for i in f)

	def __iter__(self):
		self.handler = self.source	
		self._generator = self._gen()
		self._itered = True
		return self
	
	def _gen(self):
		for geometry in self.geometry_filter:
			self.reader = pd.read_csv(self.handler, chunksize=self.chunk_size, sep=self.sep, engine='c')
			self._stopped_iteration = False
			
			try:
				while True:
					data = self.reader.get_chunk()
					data.index = self._range_index(data)

					if self.fieldnames is None: # field names not available before read # и пофиг пока
						self.fieldnames = list(data) # field names as in file, not in df :(

					if 'geometry' not in data and 'WKT' not in data:
						return data
					
					text_geometry = 'geometry' if 'geometry' in data else 'WKT'
					try:
						geom = data[text_geometry].apply(loads)
					except shapely.errors.WKTReadingError:
						# ignore bad WKT (might be not wkt at all)
						return data

					data.pop(text_geometry)
					data['geometry'] = geom

					yield gpd.GeoDataFrame(data, crs=self.crs)
			except StopIteration:
				pass


	def __next__(self):
		if not self._itered:
			iter(self)

		return next(self._generator)


class CsvWriter(DfWriter):
	def __init__(self, target):
		super().__init__(target)
		field_size_limit(10000000)

	def init_handler(self, df=None):
		if df is None:
			df = pd.DataFrame()
		from csv import DictWriter
		self.fieldnames = list(df)
		if isinstance(self.target, str):
			self._cleanup_target()
			self._handler = open(self.target, 'w')
		elif isinstance(self.target, io.TextIOWrapper):
			self._handler = self.target
		self.writer = DictWriter(self.handler, fieldnames=self.fieldnames, extrasaction='ignore')
		self.writer.writeheader()

	def writedf(self, df):
		if self._handler is None:
			self.init_handler(df)

		if 'geometry' in df:
			df['geometry'] = df['geometry'].apply(shapely.wkt.dumps)
			
		self.writer.writerows(df.to_dict(orient='records'))
		# df.to_csv(self._handler, header=False, index=False)
		self.handler.flush()


class CsvDriver(DfDriver):
	source_extension = 'csv'
	reader = CsvReader
	writer = CsvWriter


driver = CsvDriver
