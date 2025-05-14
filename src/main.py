import argparse
import time
from src.data_generator.sensor_simulator import SensorSimulator
from src.etl.extract import extract_from_kafka
from src.etl.transform import clean_sensor_data
from src.etl.load import ElasticsearchLoader

def start_simulator():
    """Démarre le simulateur de capteurs"""
    print("Démarrage du simulateur de capteurs...")
    simulator = SensorSimulator()
    simulator.run()

def start_pipeline():
    """Démarre le pipeline ETL"""
    def process_data(data):
        cleaned_data = clean_sensor_data(data)
        loader = ElasticsearchLoader()
        loader.load_data(cleaned_data)
    
    print("Démarrage du pipeline ETL...")
    extract_from_kafka(callback=process_data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='IoT ETL Pipeline')
    parser.add_argument('--mode', choices=['simulator', 'pipeline'], required=True,
                       help='Mode de fonctionnement: simulator ou pipeline')
    
    args = parser.parse_args()
    
    if args.mode == 'simulator':
        start_simulator()
    elif args.mode == 'pipeline':
        start_pipeline()