from fiona import remove, listlayers
from argh import CommandError
from shapely import wkb, wkt
import argh
import fiona
import geopandas as gpd
import inspect
import os
import pandas as pd
import pathlib
import re
import sys


#@decorator
def autoargs_once(func):
	return func


def read(fname, crs=None, driver=None, **kwargs):
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
	
	if fname is None:
		return

	match_vector = re.match(r'^(?P<filename>(?P<file_own_name>.*)\.(?P<extension>gpkg|geojson))(?:\:(?P<layer_name>[a-z0-9_]+))?$', fname)
	match_postgres = re.match(r'^postgresql\://', fname)
	match_csv = fname.endswith('.csv')


	if match_postgres:
		engine, table_or_query = _connect_postgres(fname)
		df = pd.read_sql(table_or_query, engine)

		print('Exported from Postgres.', file=sys.stderr)
		if 'geometry' in df:
			df['geometry'] = df['geometry'].apply(bytes.fromhex).apply(wkb.loads)
			return gpd.GeoDataFrame(df)

		return df

	elif fname.endswith('.json'):
		source_df = pd.read_json(fname, **kwargs)

	elif match_csv or driver == 'CSV':
		source_df = pd.read_csv(fname, **kwargs)
		if 'geometry' in source_df:
			try:
				source_df['geometry'] = source_df.geometry.apply(lambda g: wkt.loads(g))
			except AttributeError:
				print("warning: can't transform empty or broken geometry", file=sys.stderr)
			else:
				source_df = gpd.GeoDataFrame(source_df)
		if crs:
			source_df.crs = crs

	elif match_vector:
		filename = match_vector['filename']
		layer_name = match_vector['layer_name'] or match_vector['file_own_name']
		driver = 'GPKG' if match_vector['extension'] == 'gpkg' else 'GeoJSON'

		if match_vector['layer_name'] == '':
			try:
				layers = fiona.listlayers(filename)
			except ValueError as e:
				raise argh.CommandError('Fiona driver can\'t read layers from file %s' % fname)

			if len(layers) == 1 and match_vector['layer_name'] == '':
				layer_name = layers[0]
			else:
				raise argh.CommandError(f'Can\'t detect default layer in {filename}. Layers available are: {", ".join(layers)}')

		source_df = gpd.read_file(filename, driver=driver, layer=layer_name, **kwargs)
		if crs:
			source_df.crs = crs

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


__all__ = [
	# 'YANDEX', 'WGS', 'GOOGLE', 'SIB', 'crs_dict',
	'autoargs'
]

def _lazy_import(mod):
	"""
	Instead of importing target function and making it a command we make a wrapper function.
	The wrapper will import the target and wrap it as another argh command, so that
	arguments are checked and argh prints readable error messages.
	(Otherwise missing parameters will cause just an ugly traceback.)
	"""
	def _fn(*args, **kwargs):
		module = __import__(f'aktash.op.{mod}')
		fn = getattr(module.op, mod).main
		fn.__name__ = mod

		# this parser works when the "aktash <mod>" is called from shell.
		frm = inspect.stack()[1]
		mod_obj = inspect.getmodule(frm[0])

		if mod_obj is None:  #  or mod_obj.__name__ != '__main__':
			return fn(*args, **kwargs)
		
		local_parser = argh.ArghParser()
		local_parser.add_commands([fn])
		local_parser.dispatch()
	
	return _fn

g = globals()
parser = argh.ArghParser()
# scanning the directory, making lazy imports and adding them to commands
for script in (pathlib.Path(__file__).parent / 'op').iterdir():
	modname = script.stem
	if script.suffix == '.py' and not modname.startswith('__'):
		fn = _lazy_import(modname)
		# must rename function to add it correctly to commands
		fn.__name__ = modname
		g[modname] = fn
		parser.add_commands([fn])
		__all__.append(modname)

def main():
	argh.dispatching.dispatch(parser)
