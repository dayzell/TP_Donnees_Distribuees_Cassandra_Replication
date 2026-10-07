from datetime import datetime
import requests
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

# Connexion au cluster (on peut pointer sur n'importe quel nœud, ex: cass1 sur le port 9042)
cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect('velib_cluster')

# Appel de l'API Open Data Vélib' Paris
url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records?limit=20"
response = requests.get(url)
data = response.json()

print(f"Insertion de {len(data.get('results', []))} stations dans Cassandra...")

for record in data.get('results', []):
  station_id = str(record.get('stationcode'))
  nom_station = record.get('name')
  capacite = int(record.get('capacity', 0))
  vatiques_disponibles = int(record.get('numbikesavailable', 0))
  bornes_disponibles = int(record.get('numdocksavailable', 0))
  update_str = record.get('duedate')

  try:
    derniere_mise_a_jour = datetime.fromisoformat(
        update_str.replace('Z', '+00:00')
    )
  except Exception:
    derniere_mise_a_jour = datetime.now()

  # Insertion avec un niveau de cohérence QUORUM par défaut
  query = """
         INSERT INTO stations_velib (station_id, nom_station, capacite, vatiques_disponibles, bornes_disponibles, derniere_mise_a_jour)
         VALUES (%s, %s, %s, %s, %s, %s)
     """
  session.execute(
      query, (
          station_id,
          nom_station,
          capacite,
          vatiques_disponibles,
          bornes_disponibles,
          derniere_mise_a_jour,
      )
  )

print('Insertion terminée avec succès !')
cluster.shutdown()