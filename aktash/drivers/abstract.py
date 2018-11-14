#!/usr/bin/python3.6

import geopandas as gpd
import pandas as pd
import re
import shutil

class DfDriver:
	data_type = (pd.DataFrame, gpd.GeoDataFrame)
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
	def __init__(self, source, geometry_filter=None, chunk=10000, **kwargs):
		self.source = source
		self.handler = None
		self.geometry_filter = geometry_filter
		self.chunk = chunk
		self.crs = None
		self.total = None
		iter(self)

	def __iter__(self):
		return self

	def __enter__(self):
		return self

	def __exit__(self, *exc):
		if self.handler is not None:
			self.handler.close()
	
	def _make_df(self, rows):
		# some classes may return either `GeoDataFrame`, or plain `pd.DataFrame`, so let them redefine this function
		return gpd.GeoDataFrame(rows, crs=self.crs)

	def _next_row_data(self):
		# fiona drivers return structure like {'geometry': <geometry>, 'properties': <dict>}
		# this structure needs to be transformed.
		# CSV driver needs to convert geometry from WKT to shapely
		raise NotImplementedError

	def _next_row(self):
		while True:
			try:
				data = self._next_row_data()
			except StopIteration as e: # if iteration ended, close file handler
				self.__exit__()
				raise e

			if self.geometry_filter is None or data['geometry'].intersects(self.geometry_filter):
				return data

	def __next__(self):
		rows = []
		while len(rows) < self.chunk:
			try:
				rows.append(self._next_row())
			except StopIteration:
				break

		if len(rows) == 0:
			raise StopIteration

		return self._make_df(rows)


class DfWriter:
	def __init__(self, target, **kwargs):
		self.target = target
		self.fieldnames = None
		self.handler = None
		self.cleaned_up = False

	def __enter__(self):
		return self.writedf

	def __exit__(self, *exc):
		if self.handler is not None:
			self.handler.close()
			self.handler = None

	def writedf(self):
		raise NotImplementedError

	def _cleanup_target(self):
		if not self.cleaned_up:
			shutil.rmtree(self.target, True)
			self.cleaned_up = True


def rows_to_gdf(rows, crs):
	return gpd.GeoDataFrame(rows, crs=crs)
