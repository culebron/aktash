from datetime import datetime
from shapely import geometry
import csv
import fiona
import geopandas as gpd
import os
import pandas as pd
import re


def fix_multitypes(geoseries):
	"""If any object in the geoseries is multitype, converts the other ones to multitype too."""
	types = set(geoseries.geom_type.values)
	check_types = (
		('Polygon', 'MultiPolygon', geometry.MultiPolygon),
		('LineString', 'MultiLineString', geometry.MultiLineString),
		('Point', 'MultiPoint', geometry.MultiPoint)
	)

	if len(types) == 1:
		return geoseries

	for single_type, multi_type, klass in check_types:
		if types == set([single_type, multi_type]): # if it's only one kind of single/multi, then convert to multi
			return geoseries.apply(lambda g: multi_type([g]) if g.geom_type == single_type else g)

	# if no match, then it's heterogenous data, raise value error
	raise ValueError(f'geoseries contains different types of objects: {types}')


def as_gdf(df, lon_name='lon', lat_name='lat'):
	from aktash import crs
	df2 = df.copy()
	df2['geometry'] = df2.apply(lambda r: geometry.Point(r[lon_name], r[lat_name]), axis=1)
	return gpd.GeoDataFrame(df2.drop([lon_name, lat_name], axis=1), crs=crs.WGS)


FILE_INFO_DESC = {
	'size': 'File size',
	'modified': 'Modified',
	'fieldnames': 'Field names',
	'crs': 'CRS',
	'rows': 'Rows'
}

def file_info(filename):
	match_vector = re.match(r'^(?P<filename>(?P<file_own_name>.*)\.(?P<extension>gpkg|geojson|csv))(?:\:(?P<layer_name>[a-z0-9_]+))?$', filename)

	file_stats = os.stat(match_vector['filename'])
	data = {
		'size': f'{file_stats.st_size:,d}',
		'modified': datetime.fromtimestamp(file_stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S')}

	if filename.endswith('.csv'):
		with open(match_vector['filename']) as f:
			dr = csv.DictReader(match_vector['filename'])
			data['crs'] = None
			data['fieldnames'] = dr.fieldnames

		data['rows'] = sum(1 for line in f)

	else:
		layer_name = match_vector['layer_name']
		if layer_name == '':
			try:
				layers = fiona.listlayers(filename)
			except ValueError as e:
				raise ValueError('Fiona driver can\'t read layers from file %s' % match_vector['filename'])

			if len(layers) == 1:
				layer_name = layers[0]
			elif match_vector['file_own_name'] in layers:
				layer_name = match_vector['file_own_name']
			else:
				return

		driver = 'GPKG' if match_vector['extension'] == 'gpkg' else 'GeoJSON'
		with fiona.open(match_vector['filename'], layer=layer_name, driver=driver) as f:
			data.update({
				'crs': f.crs,
				'rows': len(f),
				'fieldnames': ['geometry'] + list(f.schema['properties']),
			})

	return data


def gpd_concat(gdfs, crs):
	return gpd.GeoDataFrame(pd.concat(gdfs), crs=crs)


def dict_or_json(series):
	"""
	Make sure json columns from files are loads-ed, not strings.
	"""
	from json import loads
	try:
		return series.apply(loads)
	except TypeError:
		return series


def dicts_to_json(df, inplace=False):
	"""
	Dumps json columns to text to save with geometry files.
	"""
	if inplace == False:
		df = df.copy()
	from json import dumps
	for column in df:
		v = df[df[column].notnull()][column].values
		if len(v) == 0:
			continue
		
		v = v[0]
		if isinstance(v, (dict, list)):
			df[column] = df[column].apply(dumps)

	if inplace == False:
		return df
