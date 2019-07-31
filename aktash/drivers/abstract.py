#!/usr/bin/python3.6

from collections import OrderedDict
from gistalt import AnyDataFrame
from shapely.geometry.base import BaseGeometry
import geopandas as gpd
import pandas as pd
import re
import shutil
import types


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
