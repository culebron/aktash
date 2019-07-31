#!/usr/bin/python3.6

from .abstract import DfDriver, DfReader, DfWriter
from gistalt import dicts_to_json
import geopandas as gpd
from shapely.geometry import shape


class GeoJsonReader(DfReader):
	fiona_driver = 'GeoJSON'
	
	def __init__(self, source, geometry_filter=None, chunk_size=10_000, skip=0, **kwargs):
		super().__init__(source, geometry_filter, chunk_size, skip, **kwargs)
		self._stopped_iteration = False

	def _read_schema(self):
		import fiona
		fh = fiona.open(self.source, driver=self.fiona_driver)  # self.fiona_driver because _read_schema is inherited by gpkg, etc.
		self.schema = fh.schema
		self.fieldnames = list(self.schema['properties']) + ['geometry']
		self.total = len(fh)

	def _next_row(self):
		row = next(self.row_iterator)
		data = row['properties']
		data['geometry'] = shape(row['geometry'])
		return data

	def _gen(self):
		"""This is a generator that it stored in self._generator.
		_stopped_iteration is a marker that rows are over (to fight fiona that silently restarts iteration)
		"""

		for geometry in self.geometry_filter:
			self._stopped_iteration = False
			if geometry is not None:
				self.row_iterator = self.handler.filter(mask=geometry.__geo_interface__)
			else:
				self.row_iterator = iter(self.handler)

			from tqdm import tqdm
			with tqdm(total=self.total, desc=self.source) as pbar:
				while not self._stopped_iteration:
					df = self._next_df()
					yield df
					pbar.update(len(df))

	def __iter__(self):
		import fiona
		self.handler = fiona.open(self.source, driver=self.fiona_driver)
		self.total = len(self.handler)
		self.crs = self.handler.crs
		self._generator = self._gen()
		self._itered = True
		return self


class GeoJsonWriter(DfWriter):
	fiona_driver = 'GeoJSON'

	def init_handler(self, df=None):
		import fiona
		schema = self._get_schema(df)
		# instead of self._cleanup_target(), delete fiona layer
		crs = df.crs if df is not None else None
		self._handler = fiona.open(self.target, 'w', crs=crs, driver=self.fiona_driver, schema=schema)

	def writedf(self, df):
		# we only initiate the file only when we have to write the first non-empty dataframe
		# or at the exit (we expect the output file to exist and be empty)
		if df is None or len(df) == 0:
			return
		df.drop('', axis=1, inplace=True, errors='ignore')

		if self._handler is None:
			self.init_handler(df)

		self.columns = list(df)
		self.fieldnames = list(df)

		dicts_to_json(df, inplace=True)
		if set(list(df)) == set(self.columns):
			df = df[self.columns]

		self.handler.writerecords(df.iterfeatures())


class GeoJsonDriver(DfDriver):
	data_type = gpd.GeoDataFrame
	source_extension = 'geojson'
	reader = GeoJsonReader
	writer = GeoJsonWriter

driver = GeoJsonDriver
