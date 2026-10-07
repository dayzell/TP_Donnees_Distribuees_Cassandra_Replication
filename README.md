# TP Données Distribuées — Cluster Cassandra : réplication et tolérance aux pannes

*Cassandra à 3 nœuds : RF = 3, cohérence et panne d'un nœud*

M2 Big Data & IA — Données distribuées · Joseph KEITA

Ce projet déploie un cluster Apache Cassandra de 3 nœuds avec Docker, l'alimente avec les données temps réel de l'API Vélib' de Paris, puis observe la réplication, les niveaux de cohérence et le comportement du cluster lors de la panne d'un nœud.

```text
API Vélib'  ->  import_velib.py  ->  Cluster Cassandra (3 nœuds, RF = 3)  ->  tests ONE / QUORUM / ALL + panne
```

Le compte rendu complet, avec les réponses aux questions et les captures, est dans [`docs/compte-rendu.md`](docs/compte-rendu.md).

## Architecture

```text
Cluster : tp2-cluster
└── Datacenter : dc1
    ├── Rack : rack1 ── Node : cass1   (seed, port 9042 exposé)
    ├── Rack : rack2 ── Node : cass2
    └── Rack : rack3 ── Node : cass3
```

| Paramètre | Valeur |
|---|---|
| Image | `cassandra:4.1` |
| Snitch | `GossipingPropertyFileSnitch` |
| Tokens par nœud | 16 |
| Réseau Docker | `cass-net` |
| Mémoire | 512 Mo de heap par nœud |

## Modèle de données

Keyspace `velib_cluster`, répliqué 3 fois dans `dc1` :

```sql
CREATE KEYSPACE velib_cluster
WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};

CREATE TABLE velib_cluster.stations_velib (
  station_id text,
  nom_station text,
  capacite int,
  vatiques_disponibles int,
  bornes_disponibles int,
  derniere_mise_a_jour timestamp,
  PRIMARY KEY (station_id)
);
```

- **Partition key :** `station_id`, une station = une partition = une ligne.
- **Réplication :** avec RF = 3 et 3 nœuds, chaque station est présente sur cass1, cass2 et cass3.

## Installation et lancement

Prérequis : Docker, Python 3 (testé sous WSL avec Python 3.14).

```bash
# 1. Démarrer les nœuds un par un (attendre l'état UN entre chaque)
docker compose up -d cass1
docker compose up -d cass2
docker compose up -d cass3
docker exec -it cass1 nodetool status

# 2. Créer le keyspace et la table (voir « Modèle de données »)
docker exec -it cass1 cqlsh

# 3. Environnement Python
python3 -m venv .venv
source .venv/bin/activate
pip install cassandra-driver requests pyasyncore

# 4. Importer les stations depuis l'API Vélib'
python import_velib.py

# 5. Vérifier
docker exec -it cass1 cqlsh -e "SELECT station_id, nom_station, vatiques_disponibles FROM velib_cluster.stations_velib LIMIT 5;"
```

`pyasyncore` est nécessaire avec Python 3.12 et plus : le module `asyncore`, utilisé par `cassandra-driver`, a été retiré de la bibliothèque standard.

## Scénario de test

```bash
# Réplicas d'une station
docker exec -it cass1 nodetool getendpoints velib_cluster stations_velib 15047

# Niveaux de cohérence
docker exec -it cass1 cqlsh -e "USE velib_cluster; CONSISTENCY ONE;    SELECT * FROM stations_velib WHERE station_id='15047';"
docker exec -it cass1 cqlsh -e "USE velib_cluster; CONSISTENCY QUORUM; SELECT * FROM stations_velib WHERE station_id='15047';"
docker exec -it cass1 cqlsh -e "USE velib_cluster; CONSISTENCY ALL;    SELECT * FROM stations_velib WHERE station_id='15047';"

# Panne de cass3, puis mêmes lectures + écriture en QUORUM
docker stop cass3
docker exec -it cass1 cqlsh -e "USE velib_cluster; CONSISTENCY QUORUM; INSERT INTO stations_velib (station_id, nom_station, capacite, bornes_disponibles, vatiques_disponibles, derniere_mise_a_jour) VALUES ('99999', 'Station Test Panne', 30, 15, 15, toTimestamp(now()));"

# Retour de cass3 et vérification
docker start cass3
docker exec -it cass3 cqlsh -e "USE velib_cluster; CONSISTENCY ONE; SELECT * FROM stations_velib WHERE station_id='99999';"
```

## Résultats

| Situation | ONE | QUORUM | ALL |
|---|:---:|:---:|:---:|
| 3 nœuds en ligne | ✅ | ✅ | ✅ |
| cass3 arrêté (2 réplicas sur 3) | ✅ | ✅ | ❌ `Unavailable` |
| cass3 redémarré | ✅ | ✅ | ✅ |

- Pendant la panne, `ONE` (1 réplica) et `QUORUM` (2 réplicas) restent possibles avec cass1 et cass2. `ALL` exige 3 réplicas et échoue.
- La station `99999`, écrite en `QUORUM` pendant la panne, est retrouvée directement sur cass3 après son retour, grâce au **hinted handoff** : cass1 a conservé l'écriture et l'a transmise à cass3 à son redémarrage.

## Structure du dépôt

```text
.
├── README.md
├── docker-compose.yaml     # cluster Cassandra 3 nœuds (cass1, cass2, cass3)
├── import_velib.py         # import des stations Vélib' dans velib_cluster
├── script/
│   └── getapi.py           # script d'import du TP précédent (keyspace velib)
└── docs/
    ├── compte-rendu.md     # réponses aux questions et analyse
    └── media/              # captures d'écran
```

## Nettoyage

```bash
docker compose down        # arrête le cluster, conserve les données
docker compose down -v     # arrête le cluster et supprime les volumes (données perdues)
```
