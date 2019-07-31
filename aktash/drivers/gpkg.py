#!/usr/bin/python3.6

from .geojson import GeoJsonDriver, GeoJsonReader, GeoJsonWriter
import argh
import geopandas as gpd
import os


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

driver = GpkgDriver
