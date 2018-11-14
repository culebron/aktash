from .abstract import DfDriver, DfReader, DfWriter
from aktash.utils import dicts_to_json, dict_or_json
from json import loads, decoder
import argh
import geopandas as gpd
import os
import shapely.wkt, shapely.errors, shapely.geometry


class GpkgReader(DfReader):
	fiona_driver = 'GPKG'
	
	def __init__(self, source, geometry_filter=None, chunk=None):
		super().__init__(source, geometry_filter, chunk)
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

		self.handler = fiona.open(self.source, driver=self.fiona_driver, layer=layername)
		self.total = len(self.handler)
		self.iterator = iter(self.handler)
		self.crs = self.handler.crs
		self.fieldnames = list(self.handler.schema['properties'].keys()) + ['geometry']

	def _next_row_data(self):
		record = next(self.iterator)
		data = record['properties']
		data['geometry'] = shapely.geometry.shape(record['geometry'])
		return data

	def __next__(self):
		data = dict_or_json(super().__next__())
		for c in data:
			try:
				data[c] = data[c].apply(loads)
			except (TypeError, decoder.JSONDecodeError):
				pass

		return data


class GpkgWriter(DfWriter):
	fiona_driver = 'GPKG'

	def writedf(self, df):
		df.drop('', axis=1, inplace=True, errors='ignore')
		if self.handler is None:
			self.fieldnames = list(df)

			import fiona
			from geopandas.io.file import infer_schema
			schema = infer_schema(df)

			self._cleanup_target()

			self.handler = fiona.open(self.target, 'w', crs=df.crs, driver=self.fiona_driver, schema=schema)

		dicts_to_json(df, inplace=True)
		self.handler.writerecords(df.iterfeatures())  # TODO: empty geometry/geometry collection casuses a crash. Make a silent ignore by default?


class GpkgDriver(DfDriver):
	data_type = gpd.GeoDataFrame
	source_regexp = r'^.*\.gpkg(\:.*|)$'
	reader = GpkgReader
	writer = GpkgWriter

driver = GpkgDriver
