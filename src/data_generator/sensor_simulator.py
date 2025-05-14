import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'sensor_data'
SENSOR_TYPES = ['temperature', 'humidity', 'pollution', 'light', 'noise']
QUARTIERS = ['Centre', 'Nord', 'Sud', 'Est', 'Ouest']
INTERVAL = 10  # secondes

class SensorSimulator:
    def __init__(self, broker=KAFKA_BROKER, topic=KAFKA_TOPIC):
        self.producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
        
    def generate_data(self):
        """Génère des données de capteur simulées"""
        sensor_type = random.choice(SENSOR_TYPES)
        quartier = random.choice(QUARTIERS)
        sensor_id = f"{quartier.lower()}_{sensor_type}_{random.randint(1, 5)}"
        
        # Valeurs réalistes avec possibilité occasionnelle de valeurs aberrantes
        if random.random() < 0.05:  # 5% de chance d'avoir une valeur aberrante
            if sensor_type == 'temperature':
                value = random.choice([-50, 100, 1000])
            elif sensor_type == 'humidity':
                value = random.choice([-20, 120, 500])
            elif sensor_type == 'pollution':
                value = random.choice([-10, 1000, 5000])
            elif sensor_type == 'light':
                value = random.choice([-100, 100000])
            else:  # noise
                value = random.choice([-10, 200, 1000])
        else:
            # Valeurs normales
            if sensor_type == 'temperature':
                value = round(random.uniform(-10, 40), 1)  # -10°C à 40°C
            elif sensor_type == 'humidity':
                value = round(random.uniform(20, 95), 1)  # 20% à 95%
            elif sensor_type == 'pollution':
                value = round(random.uniform(5, 150), 1)  # 5 à 150 AQI
            elif sensor_type == 'light':
                value = round(random.uniform(0, 10000), 0)  # 0 à 10000 lux
            else:  # noise
                value = round(random.uniform(30, 100), 1)  # 30 à 100 dB
        
        timestamp = datetime.now().isoformat()
        
        return {
            'sensor_id': sensor_id,
            'sensor_type': sensor_type,
            'quartier': quartier,
            'value': value,
            'unit': self._get_unit(sensor_type),
            'timestamp': timestamp
        }
    
    def _get_unit(self, sensor_type):
        """Retourne l'unité pour un type de capteur donné"""
        units = {
            'temperature': '°C',
            'humidity': '%',
            'pollution': 'AQI',
            'light': 'lux',
            'noise': 'dB'
        }
        return units.get(sensor_type, '')
    
    def send_data(self):
        """Envoie les données au topic Kafka"""
        data = self.generate_data()
        self.producer.send(self.topic, data)
        print(f"Données envoyées: {data}")
        return data
    
    def run(self, interval=INTERVAL):
        """Exécute le simulateur en boucle"""
        try:
            while True:
                self.send_data()
                time.sleep(interval)
        except KeyboardInterrupt:
            print("Arrêt du simulateur")
        finally:
            self.producer.close()

if __name__ == "__main__":
    print(f"Démarrage du simulateur de capteurs IoT (intervalle: {INTERVAL}s)")
    simulator = SensorSimulator()
    simulator.run()