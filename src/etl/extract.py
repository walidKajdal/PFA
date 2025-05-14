from src.kafka_utils.consumer import SensorDataConsumer

def extract_from_kafka(callback):
    """Extrait les données de Kafka et les envoie au callback"""
    consumer = SensorDataConsumer()
    consumer.consume(callback)

def extract_batch_from_kafka(batch_size=100, timeout=60):
    """
    Extrait un lot de données de Kafka
    
    Args:
        batch_size: Nombre de messages à extraire
        timeout: Temps d'attente maximum en secondes
        
    Returns:
        Liste de données de capteurs
    """
    data_batch = []
    
    def collect_data(data):
        data_batch.append(data)
        return len(data_batch) >= batch_size
    
    # Cette fonction serait utilisée avec un consommateur spécial
    # qui supporte la collecte par lots avec timeout
    # Pour l'implémentation actuelle, c'est un placeholder
    
    return data_batch