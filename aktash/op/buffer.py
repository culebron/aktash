#!/usr/bin/python3

from aktash import autoargs_once, crs
import geopandas as gpd
import sys

@autoargs_once
def main(input_df: gpd.GeoDataFrame, distance:float, fix_geoms:bool=True, unite:bool=False):
	"""
	Makes buffers around geometries of the input_dataframe.
	"""
	if not input_df.crs:
		print('Warning! Input_file has no CRS. Assuming 4326.', file=sys.stderr)
		input_df.crs = crs.WGS
	
	initial_crs = input_df.crs
	distance = float(distance)

	# [list...] to make it a dataframe. Otherwise changing geometry type on the fly causes exceptions.
	new_df = input_df.copy().to_crs(crs.SIB)[list(input_df)]  # list(input_file ) to transform it to pd.DataFrame. Otherwise if initial file had points or linestrings, geopandas still expects this geometry type.
	new_df['geometry'] = new_df['geometry'].buffer(distance)
	result = gpd.GeoDataFrame(new_df, crs=crs.SIB).to_crs(initial_crs)
	if fix_geoms:
		result['geometry'] = result['geometry'].buffer(0) # CRS transformation may slightly move points and make polygons self intersect. Hence buffer(0) to fix.

	if unite:
		buf_union = result.geometry.unary_union
		if buf_union.geom_type == 'Polygon':
			buf_union = [buf_union]

		return gpd.GeoDataFrame({'geometry': list(buf_union)}, crs=input_df.crs)

	return result


def real_buffer(geom, size, CRS=crs.WGS, fix_geoms=True, use_sib=True):
	if CRS == crs.SIB or not use_sib:
		return geom.buffer(size).buffer(0)
	
	tmp_geom = crs.transform_crs(geom, CRS, crs.SIB)
	buf = tmp_geom.buffer(size)
	result = crs.transform_crs(buf, crs.SIB, CRS) # making variables for debug purposes
	if fix_geoms:
		return result.buffer(0) # CRS transformation may slightly move points and make polygons self intersect. Hence buffer(0) to fix.
	return result
