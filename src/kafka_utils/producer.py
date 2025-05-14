import json
from kafka import KafkaProducer

class IoTProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='sensor_data'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
    
    def send_data(self, data):
        """Envoie les données au topic Kafka"""
        self.producer.send(self.topic, data)
        self.producer.flush()
        print(f"Données envoyées à Kafka: {data}")
        
    def close(self):
        """Ferme la connexion au producteur"""
        self.producer.close()