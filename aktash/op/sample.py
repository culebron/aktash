#!/usr/bin/python3.6

import geopandas as gpd
from aktash import autoargs_once, read, write


@autoargs_once
def main(input_df: gpd.GeoDataFrame, n, output_df):
	df = read(input_df)
	kwargs = {'frac': float(n[:-1])} if n.endswith('%') else {'n': int(float(n))} # float is safer to read from text
	write(df.sample(**kwargs), output_df)
