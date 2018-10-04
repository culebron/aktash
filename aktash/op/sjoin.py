from functools import wraps
from aktash import autoargs_once, crs
from tqdm import tqdm
import geopandas as gpd
import json

def row_crash_wrapper(func):
	@wraps(func)
	def decorated(row):
		try:
			return func(row)
		except Exception as e:
			print(row['geometry'].wkt)
			print(row)
			raise e
	return decorated

def df_crash_wrapper(func):
	@wraps(func)
	def decorated(df):
		try:
			return func(df)
		except Exception as e:
			print(df['geometry'])
			raise e

	return decorated


def _sj(left_df, right_df, how='inner', op='intersects'):
	"""Sjoins two dataframes, returning a DF of index_left and index_right columns."""
	pass



@autoargs_once
def main(main_df:gpd.GeoDataFrame, other_df:gpd.GeoDataFrame, how='inner', op='intersects', other_columns=None, final_columns=None,
	main_df_preprocessor=None, main_row_preprocessor=None,
	other_df_preprocessor=None, other_row_preprocessor=None,
	df_postprocessor=None, row_postprocessor=None,
	agg=None, remove_index_columns=True):
	"""
	Matches two dataframes and outputs main_df with fields from matching `other_df` records. If one record of `main_df` matches two or more rows in `other_df`, the row is duplicated as in relational databases.

	Parameters
	==========

	* `main_df` the dataframe whose objects will be kept in the result
	* `other_df` the dataframe from which the other attributes will be added to `main_df`
	* `how`: database-like join, either `'inner'` or `'left'` or `'right'`
	* `op` geometry operation (`'intersects'`, `'within'`)
	* `main_df_preprocessor`: function that will take `main_df` before spatial join and return
		another dataframe that will be used in the join. But output dataframe will contain rows
		and geometries from the original `main_df`.
	* `main_row_preprocessor`: function that will be applied to rows of `main_df` before spatial join.
	    Must return Shapely geometry object. This geometries will be used for spatial join,
	    but the original ones will be in the output dataframe.
	* `other_df_preprocessor`, `other_row_preprocessor` work the same way.
	* `df_postprocessor` and `row_postprocessor`: work the same way with the result dataframe before aggregation, and get a DF with `geometry` coming from `main_df`, and `geometry_other` coming from `other_df`. `df_postprocessor` should return the new (Geo)DataFrame, whereas `row_postprocessor` should return a shapely.geometry object. After applying postprocessor, `geometry_other` is discarded.
	* `agg`: (default `None`) is Pandas aggregation object. If it's provided, the result dataframe
	    will be grouped by `main_df` index, and the other columns will be aggregated according
	    to this parameter. Otherwise, rows may be repeated.

	"""

	# TODO: change row_postprocessor to return entire rows instead of geometry

	# saving original dataframes to use them for `merge`
	
	from gistalt import subset

	if remove_index_columns:
		main_df.drop(['index_main', 'index_other'], axis=1, inplace=True, errors='ignore')
		other_df.drop(['index_main', 'index_other'], axis=1, inplace=True, errors='ignore')
	elif (set(main_df) | set(other_df)) & {'index_main', 'index_other'}:
		raise ValueError('DataFrames should not have `index_main` or `index_other` columns, because this function creates them again. Please rename them to avoid collisions and confusion.')


	if how == 'cross':
		main_df_origin = gpd.GeoDataFrame(main_df.copy().reset_index(), crs=main_df.crs)
		main_df_origin['__join__'] = 1
		
		other_df_origin = gpd.GeoDataFrame(other_df.copy().reset_index(), crs=other_df.crs)
		other_df_origin['__join__'] = 1

		joined_dfs = main_df_origin.merge(other_df_origin, on='__join__', suffixes=('_main', '_other'))[['index_main', 'index_other']]

		main_df_origin.drop(['__join__', 'index'], axis=1, inplace=True)
		other_df_origin.drop(['__join__', 'index'], axis=1, inplace=True)

	else:
		main_df_origin = main_df.copy()
		other_df_origin = other_df.copy()
		# df preprocessor has priority over row preprocessor if both are provided
		if main_df_preprocessor:
			main_df = df_crash_wrapper(main_df_preprocessor)(main_df.copy())
		elif main_row_preprocessor:
			tqdm.pandas(desc='Preprocessing main rows')
			main_df = main_df.copy()
			main_df['geometry'] = main_df.apply(row_crash_wrapper(main_row_preprocessor), axis=1)

		if other_df_preprocessor:
			other_df = df_crash_wrapper(other_df_preprocessor)(other_df.copy())
		elif other_row_preprocessor:
			tqdm.pandas(desc='Preprocessing other rows')
			other_df = other_df.copy()
			other_df['geometry'] = other_df.apply(row_crash_wrapper(other_row_preprocessor), axis=1)

		# making dataframes with geometries only, for `sjoin`
		main_df_geom = gpd.GeoDataFrame(main_df.copy()[['geometry']], crs=main_df.crs)
		other_df_geom = gpd.GeoDataFrame(other_df.copy()[['geometry']], crs=other_df.crs)

		# deciding which way to `sjoin`. Left df should be smaller than the right one.
		if len(main_df_geom) <= len(other_df_geom):
			joined_dfs = gpd.sjoin(main_df_geom, other_df_geom, how=how, op=op, lsuffix='main', rsuffix='other')
			joined_dfs = joined_dfs[['index_other']].reset_index().rename(columns={'index': 'index_main'})
		else:
			if how == 'left':
				how = 'right'

			elif how == 'right':
				how = 'left'

			joined_dfs = gpd.sjoin(other_df_geom, main_df_geom, how=how, op=op, lsuffix='other', rsuffix='main')
			joined_dfs = joined_dfs[['index_main']].reset_index().rename(columns={'index': 'index_other'})


	# if other_columns is not specified, we need at least to remove 'geometry' of the other df
	if other_columns is None:
		other_columns = ';'.join(set(other_df_origin) - {'geometry'})

	result_df = main_df_origin.merge(joined_dfs, right_on='index_main', left_index=True)

	if other_columns not in (None, ''):
		other_df_origin_subset = subset(other_df_origin, other_columns)
		result_df = result_df.merge(other_df_origin_subset, left_on='index_other', right_index=True, suffixes=('', '_other'))

	if len(result_df) == 0:
		return gpd.GeoDataFrame(result_df, crs=main_df_origin.crs)
	
	if (df_postprocessor or row_postprocessor) and len(result_df) > 0:
		result_df = result_df.merge(gpd.GeoDataFrame(other_df_origin[['geometry']].rename(columns={'geometry': 'geometry_other'}), crs=other_df_origin.crs), left_on='index_other', right_index=True)  # here only the geometry_other column is merged, no need for suffixes

		if df_postprocessor:
			result_df = df_crash_wrapper(df_postprocessor)(result_df)
		else:
			tqdm.pandas(desc='Applying postprocessor')
			result_df['geometry'] = result_df.progress_apply(row_crash_wrapper(row_postprocessor), axis=1)

	if agg:
		if isinstance(agg, str):
			agg = json.loads(agg)

		agg_dict = {k: 'first' for k in list(main_df_origin)}
		agg_dict.update(agg)
		# print(list(result_df))
		# print(agg_dict)
		result_df = result_df.groupby(by=['index_main']).agg(agg_dict)

	if 'geometry' not in other_columns:
		result_df.drop('geometry_other', errors='ignore', inplace=True, axis=1)
	return gpd.GeoDataFrame(subset(result_df, final_columns), crs=main_df_origin.crs)


def distance_to_other(df, column_name='distance', processing_crs=crs.SIB):
	orig_crs = df.crs
	if orig_crs is not None:
		func = (lambda r:
			crs.transform_crs(r['geometry'], orig_crs, processing_crs).distance(
				crs.transform_crs(r['geometry_other'], orig_crs, processing_crs)))
	else:
		func = lambda r: r['geometry'].distance(r['geometry_other'])

	df[column_name] = df.apply(func, axis=1)
	return df
