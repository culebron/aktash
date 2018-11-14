from aktash import crs, write
import argh
import geopandas as gpd
from fastkml import kml


@argh.dispatch_command
def main(kmlfile, output):
	k = kml.KML()
	# open & encoding - для декодирования файлов при открытии, потому что в системе по умолчанию может стоять кодировка ascii
	with open(kmlfile, encoding='utf-8') as f:
		# а плагин сам ещё раскодирует utf-8, поэтому закодировать обратно
		k.from_string(f.read().encode('utf-8'))

	data = []
	for f in list(k.features())[0].features():
		for f2 in f.features():
			data.append({'geometry': f2.geometry, 'name': f2.name, 'category': f.name})

	gdf = gpd.GeoDataFrame(data, crs=crs.WGS)
	write(gdf, output)
