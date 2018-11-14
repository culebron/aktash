from contextlib import ExitStack
from aktash.drivers import drivers, abstract
from shapely.geometry.base import BaseGeometry
from tqdm import tqdm
import geopandas as gpd
import inspect
import pandas as pd
import types


def select_driver(source):
	for d in drivers:
		if d.can_open(source):
			return d

	raise Exception(f'No driver can open {source}')


def read_dataframe(source, geometry_filter=None, chunk=10_000, skip=0, **kwargs):
	"""
	Reads a dataframe source in chunks or in one piece.
	Returns a dataframe if `chunk` parameter is set to 'no'.

	Otherwise, returns a generator of dataframes.

	Parameters
	==========

	`source`: file name or postgres url (postgresql://host:port/db/table_or_select_query)
	`geometry_filter`: native (if possible) filter by geometry intersection. Either `shapely.geometry.base.BaseGeometry` instance, or a list of them, or DfReader instance. Default: `None`.
	`chunk`: number of records in a chunk, or 'auto', or 'no' to return the entire dataframe at once.

	If `geometry_filter` is provided, a file is sequentially opened and filtered for each geometry in the filter. Each chunk of found geometries will be matching only one geometry. But if `geometry_filter` overlap, this leads to repetition of some geometries in the chunks. Usually, we provide non-intersecting filters (regions, municipalities), hence this is usually not an issue.

	Currently, we can't ask the reader which geometry filter is used. This requires refactoring this function into an iterator class.
	"""
	if chunk == 'no':
		raise NotImplementedError

	if isinstance(source, (abstract.DfReader, types.GeneratorType)):
		yield from source
	else:
		chunk_size = int(chunk)

		if geometry_filter is None or isinstance(geometry_filter, BaseGeometry):
			geometries = [geometry_filter]
		elif isinstance(geometry_filter, str):
			geometries = (df['geometry'].values[0] for df in read_dataframe(geometry_filter, chunk=1))
		elif isinstance(geometry_filter, abstract.DfReader):
			geometries = (df['geometry'].values[0] for df in geometry_filter)
		elif inspect.isgenerator(geometry_filter):
			geometries = (r['geometry'] for df in geometry_filter for i, r in df.iterrows())
		else:
			raise ValueError('geometry filter can be: None, shapely.geometry.BaseGeometry, generator, DfReader')

		for geometry in geometries:
			driver = select_driver(source)
			rd = driver.reader(source, geometry, chunk_size, **kwargs)
			counter = tqdm(desc=str(source), total=rd.total)
			for df in rd:
				if counter.n >= skip:
					yield df
				counter.update(len(df))


def write_dataframe(data, targets):
	"""
	Writes `data` into `target` path choosing a driver automatically.

	Parameters
	==========

	* `data`: either a `pd.DataFrame` (`gpd.GeoDataFrame`) instance, or an iterable (list, generator, etc.) of such objects.
	"""

	if isinstance(targets, str):
		targets = [targets]

	if isinstance(data, pd.DataFrame):
		data = [data]
	elif inspect.isgeneratorfunction(data):
		data = data()
	elif inspect.isfunction(data):
		data = [data()]

	with ExitStack() as st:
		writers = [st.enter_context(df_writer(t)) for t in targets]

		for items in data:
			if items is None:
				continue

			if isinstance(items, pd.DataFrame):
				items = [items]

			for df, w in zip(items, writers):
				if df is not None and len(df) > 0:
					w(df)


def df_reader(source, *args, **kwargs):
	driver = select_driver(source)
	return driver.reader(source, *args, **kwargs)


def df_writer(target, *args, **kwargs):
	driver = select_driver(target)
	return driver.writer(target, *args, **kwargs)


def _extract_kwargs(source1, source2):
	sources = []
	for s in [source1, source2]:
		sources.append(s if isinstance(s, (list, tuple)) else (s, {}))

	return sources


def process_dataframes(source, target, func, **kwargs):
	write_dataframe(map(source, func, **kwargs), target)


def map(source, func, func_args, func_kwargs, **kwargs):
	for df in read_dataframe(source, **kwargs):
		res = func(df)
		if res is not None:
			yield res


def map_product_concat(source1, source2, func, chunk=1000):
	(sname1, skw1), (sname2, skw2) = _extract_kwargs(source1, source2)
	for df1 in read_dataframe(sname1, chunk=chunk, **skw1):
		for df2 in read_dataframe(sname2, chunk=chunk, **skw2):
			res = func(df1, df2)
			if res is not None:
				yield res


def map_product_reduce(source1, source2, func, reduce_func, chunk=1000):
	(sname1, skw1), (sname2, skw2) = _extract_kwargs(source1, source2)
	for df1 in read_dataframe(sname1, chunk=chunk, **skw1):
		# we assume records are grouped by left source dataframe,
		# hence we yield a dataframe for each cycle over source1
		# and group the results in between
		prev_df = None
		for df2 in read_dataframe(sname2, chunk=chunk, **skw2):
			next_df = func(df1, df2)
			if prev_df is not None:
				prev_df = reduce_func(prev_df, next_df)
			else:
				prev_df = next_df

		yield prev_df


def map_product_group(source1, source2, func, groupby, agg, chunk=1000):
	def reducer(*dfs):
		result = pd.concat(dfs).groupby(groupby).agg(agg).reset_index()
		if isinstance(dfs[0], gpd.GeoDataFrame):
			return gpd.GeoDataFrame(result, crs=dfs[0].crs)
		return result
	
	yield from map_product_reduce(source1, source2, func, reducer, chunk)
