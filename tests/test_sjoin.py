
from aktash import match, read

def test_match_points_in_polygons():
	points = read('tests/data/match-points.csv')
	simple_polys = read('tests/data/match-simple-polys.csv')
	result = match(points, simple_polys)
	print(result)

	X = result[result['name_other'] == 'X']
	assert set(X['name'].values) == {'C', 'F', 'I'}
	Y = result[result['name_other'] == 'Y']
	assert set(Y['name'].values) == {'A', 'D', 'G'}
	
	assert len(result) == 6

def test_match_points_in_polygons_with_duplicates():
	points = read('tests/data/match-points.csv')
	polys = read('tests/data/match-overlapping-polys.csv')
	result = match(points, polys)

	print(result)
	for letter in 'ACDF':
		assert len(result[result['name'] == letter]) == 2

	for letter in 'BE':
		assert len(result[result['name'] == letter]) == 1
		

def test_match_aggregation():
	points = read('tests/data/match-points.csv')
	polys = read('tests/data/match-simple-polys.csv')

	result = match(polys, points, agg={'number': 'mean', 'name_other': 'count'})
	print(result)

	assert result[result['name'] == 'X'].number.values[0] == 3
	assert result[result['name'] == 'Y'].number.values[0] == 1


def test_match_columns():
	pass