from shapely import geometry


def fix_multitypes(geoseries):
	"""If any object in the geoseries is multitype, converts the other ones to multitype too."""
	types = set(geoseries.geom_type.values)
	check_types = (
		('Polygon', 'MultiPolygon', geometry.MultiPolygon),
		('LineString', 'MultiLineString', geometry.MultiLineString),
		('Point', 'MultiPoint', geometry.MultiPoint)
	)

	if len(geoseries) == 1:
		return geoseries

	for single_type, multi_type, klass in check_types:
		if types == set([single_type, multi_type]): # if it's only one kind of single/multi, then convert to multi
			return geoseries.apply(lambda g: multi_type([g]) if g.geom_type == single_type else g)

	# if no match, then it's heterogenous data, raise value error
	raise ValueError(f'geoseries contains different types of objects: {types}')
