#!/usr/bin/python3.6

from aktash import autoargs_once, crs, match, write
from shapely.geometry import LineString, geo
from shapely.ops import unary_union, polygonize
from tqdm import tqdm
import geopandas as gpd
import numpy as np
import scipy.spatial


@autoargs_once
def main(input_df: gpd.GeoDataFrame, column=None, debug=False):
	if input_df.crs is None:  # assume 4326, WGS
		input_df.crs = crs.WGS

	original_crs = input_df.crs
	input_df = input_df.to_crs(crs.GOOGLE)
	input_df['x'] = input_df.geometry.apply(lambda g: g.x)
	input_df['y'] = input_df.geometry.apply(lambda g: g.y)
	input_df = input_df[input_df.x.notnull() & input_df.y.notnull()].copy().reset_index() # there are files with x/y == nan

	points = input_df[['x', 'y']].as_matrix()
	group_values = input_df[column] if column is not None else input_df.index.tolist()

	vor = scipy.spatial.Voronoi(points)
	b = input_df.total_bounds
	box_width = min(b[2] - b[0], b[3] - b[1])

	polylines = []
	ids = []
	center = points.mean(axis=0)
	for pointidx1, simplex in tqdm(zip(vor.ridge_points, vor.ridge_vertices), desc='Processing Voronoi diagram'):
		pointidx = pointidx1.tolist()
		categories = [group_values[pointidx[0]], group_values[pointidx[1]]]

		if categories[0] != categories[1]:
			if -1 in simplex:
				smp = np.asarray(simplex)
				i = smp[smp >= 0][0]
				t = points[pointidx[1]] - points[pointidx[0]]  # tangent
				t = t / np.linalg.norm(t)

				n = np.array([-t[1], t[0]])
				start = vor.vertices[i]
				if np.linalg.norm(start - center) > 1000000:
					continue

				far_point = vor.vertices[i] + np.sign(np.dot(start - center, n)) * n * 100000
				n2 = np.linalg.norm(far_point - start)
				if n2 > 1000000.0:
					far_point = start + (far_point - start) / n2 * 100000

				vertices = [start, far_point]

			else:
				vertices = [vor.vertices[simplex[0]], vor.vertices[simplex[1]]]

			ids.append(categories)
			polylines.append(LineString(vertices))

	bbox = geo.box(*input_df.total_bounds).buffer(box_width * .1, 0, join_style=2)

	if debug:
		write(gpd.GeoDataFrame({'geometry': polylines}), '/tmp/debug-polylines.csv')

	polylines.append(bbox.boundary)
	polygons = list(polygonize(unary_union(polylines)))
	result = gpd.GeoDataFrame({'geometry': polygons}, crs=crs.GOOGLE)
	categories = []

	subset = ['geometry']
	if column is not None:
		subset.append(column)

	result = match(result, input_df, other_columns=column, agg={column: 'first'})
	return result.to_crs(original_crs)
