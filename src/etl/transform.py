def clean_sensor_data(data):
    """Nettoie les données des capteurs en filtrant les valeurs aberrantes"""
    sensor_type = data.get('sensor_type')
    value = data.get('value')
    
    # Définition des limites pour chaque type de capteur
    limits = {
        'temperature': (-30, 50),  # -30°C à 50°C
        'humidity': (0, 100),      # 0% à 100%
        'pollution': (0, 500),     # 0 à 500 AQI
        'light': (0, 50000),       # 0 à 50000 lux
        'noise': (0, 150)          # 0 à 150 dB
    }
    
    if sensor_type in limits:
        min_val, max_val = limits[sensor_type]
        if value < min_val or value > max_val:
            print(f"Valeur aberrante détectée: {value} {data.get('unit')} pour {sensor_type}")
            # On marque comme anomalie
            data['is_anomaly'] = True
        else:
            data['is_anomaly'] = False
    
    return data

def clean_and_transform_batch(data_batch):
    """
    Nettoie et transforme un lot de données
    
    Args:
        data_batch: Liste de dictionnaires de données capteurs
        
    Returns:
        Liste de données nettoyées et transformées
    """
    return [clean_sensor_data(data) for data in data_batch]

def aggregate_data_by_quartier(data_batch, time_window='hour'):
    """
    Agrège les données par quartier et fenêtre temporelle
    
    Args:
        data_batch: Liste de dictionnaires de données capteurs
        time_window: 'minute', 'hour', 'day'
        
    Returns:
        Dictionnaire des agrégations par quartier et type de capteur
    """
    # Cette fonction serait normalement implémentée avec pandas
    # pour regrouper et agréger les données
    # Placeholder pour l'implémentation actuelle
    return {}