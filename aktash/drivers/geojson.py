from . import gpkg

class GeoJsonReader(gpkg.GpkgReader):
	fiona_driver = 'GeoJSON'


class GeoJsonWriter(gpkg.GpkgWriter):
	fiona_driver = 'GeoJSON'


class GeoJsonDriver(gpkg.GpkgDriver):
	source_extension = 'geojson'
	reader = GeoJsonReader
	writer = GeoJsonWriter

driver = GeoJsonDriver
