import pyproj
from shapely.ops import transform as _transform
from shapely import geometry
from functools import partial


YANDEX = CRS3395 = {'init': 'epsg:3395'}
GOOGLE = CRS3857 = {'init': 'epsg:3857'}
WGS = CRS4326 = {'init': 'epsg:4326'}
# RUS = '+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=105.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs'
IRK = '+proj=tmerc +lat_0=0 +lon_0=93 +k=1 +x_0=16500000 +y_0=0 +ellps=krass +towgs84=23.92,-141.27,-80.9,-0,0.35,0.82,-0.12 +units=m +no_defs'
SIB = '+proj=aea +lat_1=52 +lat_2=64 +lat_0=0 +lon_0=105 +x_0=18500000 +y_0=0 +ellps=krass +units=m +towgs84=28,-130,-95,0,0,0,0 +no_defs'
crs_dict = {4326: CRS4326, 3857: CRS3857, 3395: CRS3395, 7513: IRK, 7514: SIB}




def transform(obj, crs_from, crs_to):
	return _transform(partial(pyproj.transform, pyproj.Proj(crs_from), pyproj.Proj(crs_to)), obj)

def transform_partial(crs_from, crs_to):
	return partial(transform, partial(pyproj.transform, pyproj.Proj(crs_from), pyproj.Proj(crs_to)))

yandex2wgs = transform_partial(YANDEX, WGS)
yandex2google = transform_partial(YANDEX, GOOGLE)
google2wgs = transform_partial(GOOGLE, WGS)
google2sib = transform_partial(GOOGLE, SIB)
wgs2google = transform_partial(WGS, GOOGLE)
wgs2yandex = transform_partial(WGS, YANDEX)
wgs2sib = transform_partial(WGS, SIB)
sib2wgs = transform_partial(SIB, WGS)


meridian180 = geometry.LineString([[180, 85], [180, -85]])
meridian180sib = transform(meridian180, WGS, SIB)
meridian180sib_ribbon = meridian180sib.buffer(500) # this is the smallest width that cuts 180 meridian objects to be on separate sides.

def split_180_meridian(geom, from_crs=WGS):
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
