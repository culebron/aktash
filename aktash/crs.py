import pyproj
from shapely.ops import transform as _transform
from shapely import geometry
from functools import partial


MERC = CRS3395 = {'init': 'epsg:3395'}
GOOGLE = CRS3857 = {'init': 'epsg:3857'}
WGS = CRS4326 = {'init': 'epsg:4326'}
SIB = '+proj=aea +lat_1=52 +lat_2=64 +lat_0=0 +lon_0=105 +x_0=18500000 +y_0=0 +ellps=krass +units=m +towgs84=28,-130,-95,0,0,0,0 +no_defs'
crs_dict = {4326: CRS4326, 3857: CRS3857, 3395: CRS3395, 7514: SIB, '4326': WGS, '3857': GOOGLE, '3395': MERC, 'SIB': SIB}


def transform(obj, crs_from, crs_to):
	return _transform(partial(pyproj.transform, pyproj.Proj(crs_from), pyproj.Proj(crs_to)), obj)

merc2wgs = partial(transform, MERC, WGS)
merc2google = partial(transform, MERC, GOOGLE)
google2wgs = partial(transform, GOOGLE, WGS)
google2sib = partial(transform, GOOGLE, SIB)
wgs2google = partial(transform, WGS, GOOGLE)
wgs2merc = partial(transform, WGS, MERC)
wgs2sib = partial(transform, WGS, SIB)
sib2wgs = partial(transform, SIB, WGS)


meridian180 = geometry.LineString([[180, 85], [180, -85]])
meridian180sib = transform(meridian180, WGS, SIB)

# this is the smallest width that cuts 180 meridian objects to be on separate sides.
meridian180sib_ribbon = meridian180sib.buffer(500)

def split_180_meridian(geom, from_crs=WGS):
	# splits by 180 meridian to prevent geometries (from openstreetmap) from breaking or being weird.
	if geom.geom_type not in ('Polygon', 'MultiPolygon', 'LineString', 'MultiLineString'):
		return geom

	sib_geom = transform(geom, from_crs, SIB) if from_crs != SIB else geom
	if not sib_geom.intersects(meridian180sib):
		return geom
	
	out_geom = sib_geom.difference(meridian180sib_ribbon)
	return transform(out_geom, SIB, from_crs)


def split_180_meridian_df(df):
	if df.crs is None:
		raise ValueError('no CRS in df')

	init_crs = df.crs
	df2 = df.to_crs(SIB)
	df2['geometry'] = df2['geometry'].apply(partial(split_180_meridian, from_crs=SIB))
	return df2.to_crs(init_crs)
