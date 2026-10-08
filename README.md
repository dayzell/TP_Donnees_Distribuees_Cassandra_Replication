# TP Données Distribuées — Cassandra (réplication, pannes) et Spark

<p align="center">
  <img src="https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExNnE3cDhseWxrN3Y4b2RnYWppNGI1aTZ6ZjJsYzJiczk2NGd0Nm00aiZlcD12MV9naWZzX3JlbGF0ZWQmY3Q9Zw/l36kU80xPf0ojG0Erg/giphy.gif" alt="Vélib" width="400">
</p>

*Cassandra à 3 nœuds (RF = 3, cohérence, panne d'un nœud), puis traitement des données avec PySpark*

M2 Big Data & IA — Données distribuées · Joseph KEITA

Le projet a pour but de garantir que le service reste accessible en permanence, même si une machine tombe en panne. En répartissant le travail sur plusieurs serveurs, la défaillance de l'un d'eux n'a plus d'impact sur les utilisateurs. Cela réduit les risques de coupure et permet de faire évoluer ou réparer le système sans interrompre l'activité.

Ce projet déploie un cluster Apache Cassandra de 3 nœuds avec Docker et l'alimente avec les données temps réel de l'API Vélib' de Paris. Il observe la réplication, les niveaux de cohérence et la panne d'un nœud, puis connecte **Spark** au cluster pour traiter ces données dans un notebook PySpark.

```text
API Vélib' → import_velib.py → Cluster Cassandra (3 nœuds, RF = 3)
                                   │  cassandra-driver, réseau Docker cass-net
                                   ▼
                       Spark (Jupyter + PySpark) → DataFrame → transformations / agrégations → Spark UI
```

Le compte rendu complet, avec les réponses aux questions et les captures, est dans [`docs/compte-rendu.md`](docs/compte-rendu.md).

## Architecture

```text
Réseau Docker : cass-net
│
├── Cluster Cassandra : tp2-cluster
│   └── Datacenter : dc1
│       ├── Rack : rack1 ── Node : cass1   (seed, port 9042 exposé)
│       ├── Rack : rack2 ── Node : cass2
│       └── Rack : rack3 ── Node : cass3
│
└── spark-tp1   (Jupyter :8888, Spark UI :4040-4045, Spark 4.2.0 en local[4])
```

| Paramètre | Valeur |
|---|---|
| Image Cassandra | `cassandra:4.1` |
| Snitch | `GossipingPropertyFileSnitch` |
| Tokens par nœud | 16 |
| Mémoire | 512 Mo de heap par nœud |
| Image Spark | `quay.io/jupyter/pyspark-notebook` (Spark 4.2.0) |
| Mode Spark | `local[4]`, `spark.sql.shuffle.partitions = 4` |
| Réseau Docker | `cass-net`, commun à Cassandra et Spark |

## Connexion entre Cassandra et Spark

Spark et Cassandra tournent dans des conteneurs séparés. Ils communiquent parce qu'ils sont **sur le même réseau Docker `cass-net`** :

1. **Réseau :** le conteneur `spark-tp1` est rattaché à `cass-net`, soit par `docker network connect cass-net spark-tp1`, soit directement par le `docker-compose.yaml`. Le DNS interne de Docker résout alors `cass1`, `cass2` et `cass3`.
2. **Driver :** dans le notebook, `cassandra-driver` se connecte à `cass1` sur le port 9042 (protocole CQL natif) et découvre les deux autres nœuds. Le niveau de cohérence par défaut est `LOCAL_ONE`.
3. **Lecture :** `SELECT * FROM stations_velib` est exécuté depuis le driver Spark, puis les lignes sont converties en liste Python.
4. **DataFrame :** `spark.createDataFrame(data, columns)` répartit ces lignes en 4 partitions Spark.

```python
from cassandra.cluster import Cluster

session = Cluster(["cass1"]).connect("velib_cluster")
rows = session.execute("SELECT * FROM stations_velib")
df = spark.createDataFrame([tuple(r) for r in rows], rows.column_names)
```

Ce mode convient à un petit volume (22 stations), car toute la lecture passe par le driver Spark. Pour de gros volumes, on utiliserait le Spark Cassandra Connector, qui lit en parallèle par plages de tokens. Le détail et la comparaison sont dans la [Partie II du compte rendu](docs/compte-rendu.md#partie-ii--mode-de-connexion-entre-cassandra-et-spark).

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

Prérequis : Docker et Python 3 (testé sous WSL avec Python 3.14).

```bash
# 1. Démarrer les nœuds Cassandra un par un (attendre l'état UN entre chaque)
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
python scripts/import_velib.py

# 5. Vérifier
docker exec -it cass1 cqlsh -e "SELECT station_id, nom_station, vatiques_disponibles FROM velib_cluster.stations_velib LIMIT 5;"
```

`pyasyncore` est nécessaire avec Python 3.12 et plus : le module `asyncore`, utilisé par `cassandra-driver`, a été retiré de la bibliothèque standard.

### Lancer Spark

```bash
# 6. Démarrer le conteneur Spark, déjà relié à cass-net
docker compose up -d spark

# 7. Vérifier que Spark et les 3 nœuds sont sur le même réseau
docker network inspect cass-net \
  --format '{{range .Containers}}{{.Name}} -> {{.IPv4Address}}{{"\n"}}{{end}}'
```

Ouvrir ensuite http://localhost:8888/?token=spark. Les notebooks sont dans le dossier `work`, monté sur `notebook/`. La Spark UI est sur http://localhost:4040 tant qu'une SparkSession est active. Une deuxième session prend le port 4041.

Pour une installation manuelle, comme dans le guide du cours :

```bash
docker run -d --name spark-tp1 \
  -p 8888:8888 -p 4040-4045:4040-4045 \
  -e JUPYTER_TOKEN=spark \
  -v "$(pwd)/notebook":/home/jovyan/work \
  quay.io/jupyter/pyspark-notebook
docker network connect cass-net spark-tp1
```

Si un conteneur `spark-tp1` a déjà été créé avec `docker run`, il faut le supprimer (`docker rm -f spark-tp1`) avant `docker compose up -d spark`, car les deux utilisent le même nom.

## Partie 1 — Réplication et pannes

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

| Situation | ONE | QUORUM | ALL |
|---|:---:|:---:|:---:|
| 3 nœuds en ligne | ✅ | ✅ | ✅ |
| cass3 arrêté (2 réplicas sur 3) | ✅ | ✅ | ❌ `Unavailable` |
| cass3 redémarré | ✅ | ✅ | ✅ |

- Pendant la panne, `ONE` (1 réplica) et `QUORUM` (2 réplicas) restent possibles avec cass1 et cass2. `ALL` exige 3 réplicas et échoue.
- La station `99999`, écrite en `QUORUM` pendant la panne, est retrouvée directement sur cass3 après son retour, grâce au **hinted handoff** : cass1 a conservé l'écriture et l'a transmise à cass3 à son redémarrage.

## Partie 2 — Traitement avec Spark

Notebook : [`notebook/tp4_cassandra_spark.ipynb`](notebook/tp4_cassandra_spark.ipynb). Découverte de Spark : [`notebook/tp1_decouverte_spark.ipynb`](notebook/tp1_decouverte_spark.ipynb).

- **Sujet :** la disponibilité en temps réel des stations Vélib'.
- **Données :** `velib_cluster.stations_velib`, soit 22 lignes. L'analyse porte sur les 20 stations réelles : les 2 stations de test écrites pendant la panne sont exclues.
- **Question métier :** quelles stations sont en tension (risque d'être vides ou pleines), et combien de vélos faudrait-il déplacer ?
- **Traitements réalisés :**
  - lecture Cassandra → DataFrame Spark ;
  - sélection, filtres `< 5 vélos` et `> 20 vélos` ;
  - colonne calculée `taux_disponibilite = vélos / capacité × 100` ;
  - statistiques globales, `groupBy` par catégorie puis par niveau de tension (< 20 % : risque vide, > 80 % : risque plein) ;
  - `repartition`, démonstration de la lazy evaluation ;
  - plan d'exécution (`Exchange`) et Spark UI (Jobs, Stages, Shuffle Read / Write).

| Niveau de tension | Stations | Vélos | Bornes libres | Taux moyen |
|---|---:|---:|---:|---:|
| Risque vide | 6 | 17 | 166 | 9,3 % |
| Équilibrée | 11 | 149 | 183 | 42,7 % |
| Risque plein | 3 | 93 | 12 | 90,2 % |

Principales observations :

- **Pas de pénurie, mais une mauvaise répartition :** 259 vélos pour 647 places (40 %). 6 stations sur 20 risquent d'être vides, par exemple Morillons - Dantzig (2 vélos pour 52 places). 3 sont presque pleines, dont Messine - Place du Pérou qui n'a plus aucune borne libre. Environ 24 vélos à apporter aux stations vides, dont 9 pris sur les stations pleines, suffiraient à les rééquilibrer.
- **Partitions :** le DataFrame a 4 partitions (`local[4]`). `repartition(2)` en donne 2 de 11 lignes.
- **Lazy evaluation :** les transformations ne lancent aucun calcul. Seules les actions (`show`, `count`, `collect`) créent des Jobs : 27 sur l'ensemble du notebook.
- **Shuffle :** le `groupBy` ajoute un `Exchange hashpartitioning` dans le plan. Dans la Spark UI, il coupe le Job en deux stages : 4 tasks écrivent 392 o (*Shuffle Write*), puis 1 task relit ces 392 o (*Shuffle Read*).
- **Mode de connexion :** le plan montre `Scan ExistingRDD`. Spark lit une collection en mémoire et non Cassandra directement. Les colonnes `int` deviennent des `long`.

## Structure du dépôt

```text
.
├── README.md
├── docker-compose.yaml          # cass1, cass2, cass3 + spark (spark-tp1), réseau cass-net
├── scripts/
│   ├── import_velib.py          # import des stations Vélib' dans velib_cluster
│   └── getapi.py                # script d'import du TP précédent (keyspace velib)
├── notebook/
│   ├── tp1_decouverte_spark.ipynb   # découverte de Spark (DataFrame, partitions, lazy evaluation)
│   └── tp4_cassandra_spark.ipynb    # lecture Cassandra → DataFrame Spark → traitements
├── screenshots/
│   ├── cassandra/               # cluster, cohérence, panne et retour de cass3
│   └── spark/                   # réseau cass-net, Jupyter, Spark UI
└── docs/
    └── compte-rendu.md          # réponses aux questions, connexion Cassandra ↔ Spark, analyse
```

Les énoncés et guides PDF du cours sont rangés localement dans `docs/enonces/`. Ce dossier est exclu du dépôt par le `.gitignore`.

## Nettoyage

```bash
docker compose down        # arrête Cassandra et Spark, conserve les données
docker compose down -v     # arrête tout et supprime les volumes (données Cassandra perdues)
```

---

<p align="center">
  <img src="docs/media/velib_spiderman.png" alt="Spark et Cassandra gardiens du réseau Vélib'" width="100%">
</p>