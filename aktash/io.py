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


def stream_writer(target, *args, **kwargs):
	"""Chooses driver for target. Internal function used in read_dataframes."""
	driver = select_driver(target)
	return driver.writer(target, *args, **kwargs)


def stream_reader(source, geometry_filter=None, chunk_size=10_000, skip=0, **kwargs):
	if isinstance(source, (abstract.DfReader, types.GeneratorType)):
		return source

	if isinstance(source, pd.DataFrame):
		return [source]

	# if one df, make a wrapper driver
	driver = select_driver(source)
	return driver.reader(source, geometry_filter, chunk_size, skip, **kwargs)
