import requests
from cassandra.cluster import Cluster
 
url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records?limit=100"
stations = requests.get(url).json()["results"]
 
session = Cluster(["127.0.0.1"]).connect("velib")
 
insert = session.prepare("INSERT INTO stations (station_id, name, capacity, latitude, longitude) VALUES (?, ?, ?, ?, ?)")
for s in stations:
    geo = s.get("coordonnees_geo") or {}
    session.execute(insert, (str(s["stationcode"]), s["name"], s["capacity"], geo.get("lat"), geo.get("lon")))
 
print(len(stations), "stations insérées")
 
