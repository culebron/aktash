#!/usr/bin/python3.6

from gistalt import read_file, crs
from gistalt.op import cut_lines
from shapely.geometry import LineString

def _gd(df):
	# turns id-geometry dataframe into dictionary
	return _vd(df, 'id', 'geometry')

def _vd(df, keys, vals):
	# turns any pair of columns into dict
	return dict(df[[keys, vals]].values)

def test_cut_lines():
	lines = read_file('tests/data/cut-lines.csv')
	lines.crs = crs.WGS
	result = cut_lines.main(lines, end=5, skip_conversion=True)
	
	data = _gd(result)

	assert data[1] == LineString([[0, 0], [1, 0]])
	assert data[2] == data[3] == data[4] == LineString([[0, 0], [5, 0]])
	

	result = cut_lines.main(lines, end=-1, skip_conversion=True)

	# making lengths dictionary, to check if all lengths are exactly shorter by 1
	lines['line_length'] = lines.length
	len_dict = _vd(lines, 'id', 'line_length')

	result['line_length'] = result.length
	data = _gd(result)

	assert 1 not in data
	assert data[2].wkt == "LINESTRING (0 0, 9 0)"
	assert data[3].wkt == "LINESTRING (0 0, 5 0, 9 0)"

	len_dict2 = _vd(result, 'id', 'line_length')
	
	for k, v in len_dict2.items():
		assert len_dict[k] == v + 1

	result  = cut_lines.main(lines, end=-1, skip_conversion=True, keep_empty=True)
	assert 1 in _gd(result)


def test_arrange_position():
	ap = cut_lines.arrange_position
	pairs = ((10, 10), (0, 0), (-1, 99), (-100, 0), (-200, 0), (200, 100))
	for a, b in pairs:
		assert ap(100, a) == b

def test_cut_line():
	init = LineString([[0, 0], [10, 0]])

	def clls(d):
		return cut_lines.cut_line(init, d)

	def ls(*items):
		return LineString(items)

	def pair_assert(a, b):
		for x, y in zip(a, b):
			assert x == y

	pair_assert(clls(2.5), [ls([0, 0], [2.5, 0]), ls([2.5, 0], [10, 0])])
	pair_assert(clls(-2.5), [ls([0, 0], [7.5, 0]), ls([7.5, 0], [10, 0])])
	pair_assert(clls(20), [ls([0, 0], [10, 0]), ls()])
	pair_assert(clls(0), [ls(), ls([0, 0], [10, 0])])
	pair_assert(clls(-20), [ls(), ls([0, 0], [10, 0])])
	pair_assert(clls(None), [ls([0, 0], [10, 0]), ls([0, 0], [10, 0])])

