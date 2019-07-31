from .crs import WGS, GOOGLE, MERC, SIB, crs_dict
from argh import CommandError
from fiona import remove, listlayers
from shapely import wkb, wkt
import argh
import fiona
import geopandas as gpd
import os
import pandas as pd
import re
import sys

GEOJSON_NAME = r'^(?P<filename>(?:.*/)?(?P<file_own_name>.*)\.(?P<extension>geojson))$'
GPKG_NAME = r'^(?P<filename>(?:.*/)?(?P<file_own_name>.*)\.(?P<extension>gpkg))(?:\:(?P<layer_name>[a-z0-9_-]+))?$'
POSTGRES_URL = r'^postgresql\://'

AKDEBUG = (os.environ.get('AKDEBUG') == '1')


#@decorator
def autoargs_once(func):
	return func


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
               raise CommandError('Use this format to read from sql: postgresql://[user[:password]@]hostname[:port]/<db_name>#<table_name or query>.')

       sharp_idx = path_string.index('#')
       engine = create_engine(path_string[:sharp_idx])
       return engine, path_string[sharp_idx+1:]


def _try_gdf(source_df, crs=None):
	if 'geometry' in source_df:
		source_df = source_df[source_df['geometry'].notnull()]
		source_df['geometry'] = source_df.geometry.apply(wkt.loads)
		source_df = gpd.GeoDataFrame(source_df, crs=crs)
	return source_df


__all__ = [
	'MERC', 'WGS', 'GOOGLE', 'SIB', 'crs_dict'
]

