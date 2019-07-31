#!/usr/bin/python3

from collections import OrderedDict
from csv import field_size_limit
from fiona import remove, listlayers
from gistalt import AnyDataFrame
from shapely import wkb, wkt
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry
import argh
import fiona
import geopandas as gpd
import io
import os
import pandas as pd
import re
import shapely.errors
import shutil
import sys
import types


GEOJSON_NAME = r'^(?P<filename>(?:.*/)?(?P<file_own_name>.*)\.(?P<extension>geojson))$'
GPKG_NAME = r'^(?P<filename>(?:.*/)?(?P<file_own_name>.*)\.(?P<extension>gpkg))(?:\:(?P<layer_name>[a-z0-9_-]+))?$'
POSTGRES_URL = r'^postgresql\://'

AKDEBUG = (os.environ.get('AKDEBUG') == '1')


def read(filename, crs=None, driver=None, **kwargs):
	"""
	Reads DataFrame or GeoDataFrame from file or postgres database, judging by file extension or `driver` parameter.

	Parameters
	----------

	* `fname` - name or path to file to read. The function tries to detect format by extension.
	* `crs` - number of CRS in CRS_DICT.
	* `driver` - explicitly specifies Fiona driver. Works only for CSV or GeoJSON.

	Retruns `pandas.DataFrame`, or `geopandas.GeoDataFrame`.

	Specify `driver` if extension does not tell the format. E.g. `read_file('my_file.txt', driver='CSV')`. It's not guaranteed that driver will override the extension. Files are parsed in arbitrary order, which that its not guaranteed that you can override `.geojson` extension with `driver='CSV'` parameter. See the source code or call (Geo)Pandas `read_file` directly if you have such edge cases.
	"""
	
	match_gpkg = re.match(GPKG_NAME, filename)
	match_geojson = re.match(GEOJSON_NAME, filename)
	match_postgres = re.match(POSTGRES_URL, filename)
	match_csv = filename.endswith('.csv')

	if match_postgres:
		engine, table_or_query = _connect_postgres(filename)
		df = pd.read_sql(table_or_query, engine)

		print('Exported from Postgres.', file=sys.stderr)
		if 'geometry' in df:
			df['geometry'] = df['geometry'].apply(bytes.fromhex).apply(wkb.loads)
			return gpd.GeoDataFrame(df)

		return df

	elif match_csv or driver == 'CSV':
		source_df = pd.read_csv(filename, **kwargs)
		if 'geometry' in source_df or 'WKT' in source_df:  # WKT is column name from QGIS
			text_geometry = 'geometry' if 'geometry' in source_df else 'WKT'
			try:
				geoseries = source_df[text_geometry].apply(wkt.loads)
			except AttributeError:
				print("warning: can't transform empty or broken geometry", file=sys.stderr)
			else:
				source_df.pop(text_geometry)
				source_df['geometry'] = geoseries
				source_df = gpd.GeoDataFrame(source_df)
		if crs:
			source_df.crs = crs

		return source_df

	elif match_geojson:
		source_df = gpd.read_file(filename)
		if crs is not None:
			source_df.crs = crs

		return source_df

	elif match_gpkg:
		filename = match_gpkg['filename']
		layer_name = match_gpkg['layer_name']
		driver = 'GPKG' if match_gpkg['extension'] == 'gpkg' else 'GeoJSON'

		if match_gpkg['layer_name'] in ('', None):
			try:
				layers = fiona.listlayers(filename)
			except ValueError as e:
				raise argh.CommandError('Fiona driver can\'t read layers from file %s' % filename)

			if len(layers) == 1 and match_gpkg['layer_name'] in ('', None):
				layer_name = layers[0]
			elif match_gpkg['file_own_name'] in layers:
				layer_name = match_gpkg['file_own_name']
			else:
				raise argh.CommandError('Can\'t detect default layer in %s. Layers available are: %s' % (filename, ', '.join(layers)))

		with fiona.open(filename) as f:
			rows = len(f)
		if rows == 0:
			return gpd.GeoDataFrame()

		source_df = gpd.read_file(filename, driver=driver, layer=layer_name, **kwargs)
		if crs:
			source_df.crs = crs

	elif '.xls' in filename or '.xlsx' in filename:
		if '.xls:' in filename or '.xlsx:' in filename:
			try:
				filename, sheet_name = filename.split(':')
			except ValueError as e:
				raise argh.CommandError('File name should be name.xls[x] or name.xls[x]:sheet_name. Got "%s" instead.' % filename)
		else:
			sheet_name = None

		excel_dict = pd.read_excel(filename, sheet_name=sheet_name)  # OrderedDict of dataframes
		source_df = _try_gdf(excel_dict.popitem(False)[1])  # pop item, last=False, returns (key, value) tuple

	return source_df


def write(df, fname):
	"""
	Writes [Geo]DataFrame to file or database. Format (driver) is detected by the fname parameter.

	'*.csv' => csv (geometry is transformed to WKT)
	'*.geojson' => GeoJSON
	'*.gpkg[:<layer_name>]' => GeoPackage (GPKG)
	'postgresql://' => Postgresql table (postgresql://[user[:password]@]hostname[:port]/<db_name>#<table_name or query>)
	"""
	from . import utils
	if isinstance(df, gpd.GeoDataFrame) and len(df) > 0:
		df['geometry'] = utils.fix_multitypes(df['geometry'])

	match_gpkg = re.match(r'^(?P<filename>.*/(?P<file_own_name>.*)\.(?P<extension>gpkg))(?:\:(?P<layer_name>[a-z0-9_]+))?$', fname)
	match_geojson = re.match(r'^(?P<filename>.*/(?P<file_own_name>.*)\.(?P<extension>gpkg))$', fname)
	match_postgres = re.match(r'^postgresql\://', fname)
	match_csv = fname.endswith('.csv')

	if match_postgres:
		engine, table_name = _connect_postgres(fname)
		df = df[list(df)]

		with engine.begin() as connection:
			if 'geometry' in df:
				df['geometry'] = df['geometry'].apply(wkb.dumps).apply(bytes.hex)

			pd.io.sql.execute('DROP TABLE IF EXISTS %s' % table_name, connection)
			df.to_sql(table_name, connection, chunksize=1000)
			if 'geometry' in df:
				if not df.crs and -181 < df['geometry'].extents[0] < 181:
					crs_num = 4326
				elif not df.crs:
					crs_num = 3857
				else:
					crs_num = re.match(r'.*\:(\d+)', df.crs['init'])[1]

				pd.io.sql.execute("""
					ALTER TABLE %s
					ALTER COLUMN "geometry" TYPE Geometry""" % table_name, connection)
				pd.io.sql.execute("""
					UPDATE %s SET "geometry"=st_setsrid(geometry, %s)
					""" % (table_name, crs_num), connection)


	elif isinstance(df, gpd.GeoDataFrame):
		if match_csv:
			if os.path.exists(fname):
				os.unlink(fname)
			df = pd.DataFrame(df)
			df.to_csv(fname, index=False)

		elif match_gpkg:
			filename = match_gpkg['filename']
			layer_name = match_gpkg['layer_name'] or match_gpkg['file_own_name']
			
			if os.path.exists(filename):
				if layer_name in listlayers(filename):
					remove(filename, 'GPKG', layer_name)

			df.to_file(filename, driver='GPKG', encoding='utf-8', layer=layer_name)
		elif match_geojson:
			if os.path.exists(filename):
				os.unlink(filename)

			df.to_file(filename, driver='GeoJSON', encoding='utf-8')

	elif isinstance(df, pd.DataFrame):
		if fname.endswith('.json'):
			df.to_json(fname)
		else:
			df.to_csv(fname, index=False)


def _connect_postgres(path_string):
       from sqlalchemy import create_engine

       if '#' not in path_string:
               raise argh.CommandError('Use this format to read from sql: postgresql://[user[:password]@]hostname[:port]/<db_name>#<table_name or query>.')

       sharp_idx = path_string.index('#')
       engine = create_engine(path_string[:sharp_idx])
       return engine, path_string[sharp_idx+1:]


def _try_gdf(source_df, crs=None):
	if 'geometry' in source_df:
		source_df = source_df[source_df['geometry'].notnull()]
		source_df['geometry'] = source_df.geometry.apply(wkt.loads)
		source_df = gpd.GeoDataFrame(source_df, crs=crs)
	return source_df


def open_stream(source, geometry_filter=None, chunk_size=10_000, skip=0, **kwargs):
	if isinstance(source, (DfReader, types.GeneratorType)):
		return source

	if isinstance(source, pd.DataFrame):
		return [source]

	# if one df, make a wrapper driver
	driver = select_driver(source)
	return driver.reader(source, geometry_filter, chunk_size, skip, **kwargs)


def open_write(target, *args, **kwargs):
	"""Chooses driver for target. Internal function used in read_dataframes."""
	driver = select_driver(target)
	return driver.writer(target, *args, **kwargs)


def select_driver(source):
	"""Chooses driver module for source/target. Internal function used by io.read/write_dataframes."""
	for d in drivers:
		if d.can_open(source):
			return d

	raise Exception(f'No driver can open {source}')


drivers = []

class DfDriver:
	data_type = (AnyDataFrame, pd.DataFrame, gpd.GeoDataFrame)
	source_extension = None
	source_regexp = None

	@classmethod
	def can_open(cls, source):
		if cls.source_extension is not None:
			return source.endswith('.' + cls.source_extension)

		if cls.source_regexp is not None:
			return re.match(cls.source_regexp, source)

		return False


class DfReader:
	def __init__(self, source, geometry_filter=None, chunk_size=10_000, skip=0, **kwargs):
		from gistalt.io import open_stream
		self.index_start = 0
		self.source = source
		self.handler = None
		self.skip = 0  # как skip, если фильтр по геометрии?

		if geometry_filter is None or isinstance(geometry_filter, BaseGeometry):
			g_ = [geometry_filter]
		elif isinstance(geometry_filter, str):
			g_ = (df['geometry'].values[0] for df in open_stream(geometry_filter, chunk_size=1))
		elif isinstance(geometry_filter, (DfReader, types.GeneratorType)):
			g_ = (g for df in geometry_filter for g in df['geometry'].values)
		elif isinstance(geometry_filter, gpd.GeoSeries):
			g_ = geometry_filter.tolist()
		elif isinstance(geometry_filter, gpd.GeoDataFrame):
			g_ = geometry_filter['geometry'].tolist()
		else:
			raise ValueError('geometry filter can be: None, shapely.geometry.BaseGeometry, generator, DfReader')

		self.fieldnames = None
		self.schema = None
		self.geometry_filter = g_
		self.chunk_size = chunk_size
		self.crs = None
		self.total = None
		self._generator = None
		self._stopped_iteration = False  # used in fiona drivers that raises StopIteration, but next time restarts
		self._itered = False
		self._read_schema()

	def _read_schema(self):
		raise NotImplementedError

	def __iter__(self):
		# open to read
		raise NotImplementedError

	def __enter__(self):
		iter(self) # ?
		return self

	def __exit__(self, *exc):
		if self.handler is not None:
			self.handler.close()

	def __next__(self):
		if not self._itered:
			iter(self)

		if self.handler and self.handler.closed:
			raise StopIteration

		return next(self._generator)

	def _next_df(self):
		rows = []
		while len(rows) < self.chunk_size:
			try:
				rows.append(self._next_row())
			except StopIteration:
				self._stopped_iteration = True
				break

		if len(rows) == 0:
			raise StopIteration

		#return self._make_df(rows)
		return gpd.GeoDataFrame(rows, crs=self.crs, index=self._range_index(rows))

	def _next_row(self):
		try:
			result = next(self.row_iterator)
		except StopIteration as e:
			raise e

		return result

	def _range_index(self, rows):
		start = self.index_start
		end = start + len(rows)
		self.index_start = end
		return pd.RangeIndex(start, end)


class DfWriter:
	def __init__(self, target, **kwargs):
		self.target = target
		self.fieldnames = None
		self._handler = None
		self.cleaned_up = False

	def __enter__(self):
		return self.writedf

	def _get_schema(self, df):
		from geopandas.io.file import infer_schema
		if df is None or len(df) == 0:
			# write empty dataframe to file anyway
			# TODO: if empty df will be written initially, the schema may have incorrect geometry type
			schema = {'geometry': 'Point', 'properties': OrderedDict()}
		else:
			schema = infer_schema(df)			
		return schema

	def init_handler(self, df=None):
		raise NotImplementedError

	@property
	def handler(self):
		if self._handler is None:
			raise RuntimeError('handler should be initialized with a dataframe before accessing it, but it wasn\'t')
		return self._handler

	def __exit__(self, *exc):
		if self._handler is None:
			self.init_handler()
		self._handler.flush()
		self._handler.close()
		
	def writedf(self, df):
		raise NotImplementedError

	def _cleanup_target(self):
		if not self.cleaned_up:
			shutil.rmtree(self.target, True)
			self.cleaned_up = True


def rows_to_gdf(rows, crs):
	return gpd.GeoDataFrame(rows, crs=crs)


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
			self.schema['geometry'] = wkt.loads(df[geom_col][0]).geom_type

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
						geom = data[text_geometry].apply(wkt.loads)
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


class GpkgReader(GeoJsonReader):
	fiona_driver = 'GPKG'

	def __str__(self):
		return f'GpkgReader of \'{self.source}\' ({self.geometry_filter}, {self.chunk_size})'
	
	def __repr__(self):
		return f'GpkgReader of \'{self.source}\' ({self.geometry_filter}, {self.chunk_size})'
		
	def __init__(self, source, geometry_filter=None, chunk_size=10_000, skip=0, **kwargs):
		super().__init__(source, geometry_filter, chunk_size, skip, **kwargs)
		import fiona
		layername = None
		if '.gpkg:' in self.source:
			try:
				self.source, layername = self.source.split(':')
			except ValueError as e:
				raise argh.CommandError('File name should be name.gpkg or name.gpkg:layer_name. Got "%s" instead.' % self.source)
		else:
			try:
				layers = fiona.listlayers(self.source)
			except ValueError as e:
				raise argh.CommandError('Fiona driver can\'t read layers from file %s' % self.source)

			if len(layers) == 1:
				layername = layers[0]

			else:
				layername = os.path.splitext(os.path.basename(self.source))[0]
				if layername not in layers:
					raise argh.CommandError('Can\'t detect default layer in %s. Layers available are: %s' % (self.source, ', '.join(layers)))

		self.layername = layername

	def __iter__(self):
		import fiona
		self.handler = fiona.open(self.source, driver=self.fiona_driver, layer=self.layername)
		self.total = len(self.handler)
		self.crs = self.handler.crs
		self._generator = self._gen()
		self._itered = True
		return self


class GpkgWriter(GeoJsonWriter):
	fiona_driver = 'GPKG'

	def init_handler(self, df=None):
		import fiona
		schema = self._get_schema(df)

		layername = None
		if '.gpkg:' in self.target:
			try:
				self.filename, layername = self.target.split(':')
			except ValueError as e:
				raise argh.CommandError('File name should be name.gpkg or name.gpkg:layer_name. Got "%s" instead.' % self.target)
		else:
			self.filename = self.target
			layername = os.path.splitext(os.path.basename(self.target))[0]

		# instead of self._cleanup_target(), delete fiona layer
		if os.path.exists(self.filename) and layername in fiona.listlayers(self.filename):
			fiona.remove(self.filename, self.fiona_driver, layername)

		crs = df.crs if df is not None else None
		self._handler = fiona.open(self.filename, 'w', crs=crs, driver=self.fiona_driver, schema=schema, layer=layername)


class GpkgDriver(GeoJsonDriver):
	data_type = gpd.GeoDataFrame
	source_regexp = r'^.*\.gpkg(\:.*|)$'
	source_extension = None
	reader = GpkgReader
	writer = GpkgWriter


drivers = [CsvDriver, GeoJsonDriver, GpkgDriver]
