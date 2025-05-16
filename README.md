# IoT ETL Pipeline
Un pipeline ETL pour l'analyse de données de capteurs IoT dans le contexte d'une ville intelligente.

## Description du projet
Ce projet implémente un pipeline ETL (Extract, Transform, Load) pour traiter les données de capteurs IoT en temps réel. Les données simulées incluent des mesures de température, humidité, pollution, luminosité et bruit provenant de différents quartiers d'une ville.

## Fonctionnalités

- Génération de données simulées de capteurs IoT
- Pipeline ETL en temps réel
- Détection et marquage des valeurs aberrantes
- API pour consulter les données brutes et agrégées
- Visualisation via Kibana (Elasticsearch)
- Orchestration du pipeline avec Airflow

## Structure du projet
```
iot_etl_pipeline/
├── docker-compose.yml          # Services d'infrastructure (Kafka, Elasticsearch, etc.)
├── requirements.txt            # Dépendances Python
├── src/
│   ├── data_generator/         # Simulation des capteurs IoT
│   ├── kafka_utils/            # Utilitaires pour Kafka
│   ├── etl/                    # Logique ETL (Extract, Transform, Load)
│   ├── api/                    # API FastAPI
│   ├── dags/                   # DAGs Airflow
│   └── main.py                 # Point d'entrée principal
└── data/                       # Stockage temporaire
```

## Prérequis

- Python 3.8+
- Docker et Docker Compose

## Installation

Cloner le dépôt :

```
git clone https://github.com/walidKajdal/iot_etl_pipeline.git
cd iot_etl_pipeline
```

Créer un environnement virtuel :

```
python -m venv venv
source venv/bin/activate  # Sur Windows : venv\Scripts\activate
```

Installer les dépendances :

```
pip install -r requirements.txt
```

Démarrer les services d'infrastructure :

```
docker-compose up -d
```

## Utilisation

Lancer le simulateur de capteurs :

```
python -m src.main --mode simulator
```

Dans un autre terminal, lancer le pipeline ETL :

```
python -m src.main --mode pipeline
```

Démarrer l'API :

```
uvicorn src.api.main:app --reload
```

Accéder à l'API sur http://localhost:8000/docs  
Visualiser les données via Kibana sur http://localhost:5601

## Points d'accès API

- GET /sensors - Liste tous les capteurs disponibles
- GET /quartiers - Liste tous les quartiers disponibles
- GET /data - Récupère les données des capteurs (filtres disponibles)
- GET /aggregate - Agrège les données des capteurs selon différentes dimensions

## Airflow (optionnel)
Pour utiliser Airflow pour orchestrer le pipeline :

Initialiser Airflow :

```
export AIRFLOW_HOME=./airflow
airflow db init
```

Créer un utilisateur admin :

```
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

Démarrer le serveur web et le scheduler :

```
airflow webserver -p 8080
airflow scheduler
```

Accéder à l'interface Airflow sur http://localhost:8080
