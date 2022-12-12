import folium
import pandas as pd

map = folium.Map(location = [41.39548348445511, 2.170658111572266], zoom_start = 12)
folium.GeoJson("neighbourhoods.geojson", name = "Barcelona").add_to(map)

map.save('output.html')

url = ("https://storage.googleapis.com/kagglesdsdata/datasets/417555/797992/neighbourhoods.geojson?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20221212%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20221212T014801Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=5b6fd572b10443fe1bb9a6942a02fcd608dc39fdeaf613db7e7414d3d8a794f47e86abff2d75b993d02c3e105ef48534eb663383bab6776480790f87812e9058864702fa587d86d5591a67497774a53c0ad77b9f2bb7f4204a0c77dd2dee219e38058fc30f574e1e141e963d2d997c2cfcd8365b89a7af1ec8effb51e1a06bce75e429624fb41fb071767672e0980d6e3fce629b91f879b23e8fb2564ec2f55ba9e8be6b7e1e737f3c20bf3ce46cb938372c604216fc47910aa07e23bcb768ac4c5467ec427d29d80c201ecea1c7cc95ba7f6358eb49e748755de05ad5df613f6f055c43df266591c70d62109d21e7e4439e32f242e914e39973dd82001967f6")
barcelona_geo = "neighbourhoods.geojson"
barcelona_data = pd.read_csv('precios.csv')

map2 = folium.Map(location = [41.39548348445511, 2.170658111572266], zoom_start = 12)
             
folium.Choropleth(
	geo_data=barcelona_geo,
	name="choropleth",
	data=barcelona_data,
	columns=['neighbourhood_cleansed', 'averagePrice'],
	key_on="features.properties.neighbourhood",
	fill_color="YlGn",
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name="Unemployment Rate (%)",
).add_to(map2)

folium.LayerControl().add_to(map2)
           
map2.save('pintado.html')
