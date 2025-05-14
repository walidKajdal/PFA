import json
from kafka import KafkaConsumer

class SensorDataConsumer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='sensor_data'):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    
    def consume(self, callback=None):
        """
        Consomme les messages du topic Kafka
        Si callback est fourni, il sera appelé avec le message comme argument
        """
        try:
            for message in self.consumer:
                data = message.value
                print(f"Message reçu: {data}")
                
                if callback:
                    callback(data)
                    
        except KeyboardInterrupt:
            print("Arrêt du consommateur")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    # Test simple
    def print_data(data):
        print(f"Traitement des données: {data}")
    
    consumer = SensorDataConsumer()
    consumer.consume(callback=print_data)