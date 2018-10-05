import pandas as pd
from datetime import datetime
from shapely import geometry
import csv
import fiona
import geopandas as gpd
import os


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
	return gpd.GeoDataFrame(df2.drop(['lon', 'lat'], axis=1), crs=crs.WGS)


def file_info(filename):
	file_stats = os.stat(filename)
	data = {
		'size': f'{file_stats.st_size:,d}',
		'modified': datetime.fromtimestamp(file_stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S')}

	if filename.endswith('.csv'):
		with open(filename) as f:
			dr = csv.DictReader(filename)
			data['crs'] = None
			data['fieldnames'] = dr.fieldnames

		data['rows'] = sum(1 for line in f)

	else:
		with fiona.open(filename) as f:
			data.update({
				'crs': f.crs,
				'rows': len(f),
				'fieldnames': ['geometry'] + list(f.schema['properties']),
			})

	return data


def gpd_concat(gdfs, crs):
	return gpd.GeoDataFrame(pd.concat(gdfs), crs=crs)
