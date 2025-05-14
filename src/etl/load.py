from elasticsearch import Elasticsearch

class ElasticsearchLoader:
    def __init__(self, host='localhost', port=9200):
        self.es = Elasticsearch([f'http://{host}:{port}'])
        self._create_index_if_not_exists()
    
    def _create_index_if_not_exists(self):
        """Crée l'index Elasticsearch s'il n'existe pas"""
        if not self.es.indices.exists(index='sensor_data'):
            self.es.indices.create(
                index='sensor_data',
                body={
                    'mappings': {
                        'properties': {
                            'sensor_id': {'type': 'keyword'},
                            'sensor_type': {'type': 'keyword'},
                            'quartier': {'type': 'keyword'},
                            'value': {'type': 'float'},
                            'unit': {'type': 'keyword'},
                            'timestamp': {'type': 'date'},
                            'is_anomaly': {'type': 'boolean'}
                        }
                    }
                }
            )
            print("Index 'sensor_data' créé")
    
    def load_data(self, data):
        """Charge les données dans Elasticsearch"""
        try:
            self.es.index(index='sensor_data', body=data)
            print(f"Données chargées dans Elasticsearch: {data['sensor_id']}")
        except Exception as e:
            print(f"Erreur lors du chargement dans Elasticsearch: {e}")
    
    def load_batch(self, data_batch):
        """
        Charge un lot de données dans Elasticsearch
        
        Args:
            data_batch: Liste de dictionnaires de données capteurs
        """
        if not data_batch:
            print("Aucune donnée à charger")
            return
            
        operations = []
        for data in data_batch:
            # Prépare les opérations en bulk
            operations.append({"index": {"_index": "sensor_data"}})
            operations.append(data)
        
        try:
            if operations:
                self.es.bulk(operations)
                print(f"Lot de {len(data_batch)} données chargées dans Elasticsearch")
        except Exception as e:
            print(f"Erreur lors du chargement en lot dans Elasticsearch: {e}")

def load_to_elasticsearch(data_batch):
    """
    Fonction d'utilitaire pour charger des données dans Elasticsearch
    
    Args:
        data_batch: Liste de dictionnaires de données capteurs
    """
    loader = ElasticsearchLoader()
    loader.load_batch(data_batch)