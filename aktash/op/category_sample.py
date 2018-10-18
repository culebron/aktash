#!/usr/bin/python3.6

from aktash import autoargs_once, read, write
import geopandas as gpd
import pandas as pd


@autoargs_once
def main(input_df: gpd.GeoDataFrame, category_column, n, output_df):
	"""Selects n or n% of categories from dataframe and returns all rows of these categories."""
	df = read(input_df)
	#categories = df[category_column].astype('category')
	kwargs = {'frac': float(n[:-1])} if n.endswith('%') else {'n': int(float(n))} # float is safer to read from text
	keys = pd.Series(df[category_column].unique()).sample(**kwargs).tolist()
	write(df.set_index(category_column).loc[keys].reset_index(), output_df)
