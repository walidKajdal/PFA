from fastapi import FastAPI, Query
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
from typing import List, Optional
import json

app = FastAPI(title="IoT Sensor Data API")

es = Elasticsearch(['http://localhost:9200'])

@app.get("/")
def read_root():
    return {"message": "Welcome to IoT Sensor Data API"}

@app.get("/sensors")
def get_sensors():
    """Récupère la liste de tous les capteurs disponibles"""
    query = {
        "size": 0,
        "aggs": {
            "sensor_ids": {
                "terms": {
                    "field": "sensor_id",
                    "size": 1000
                }
            }
        }
    }
    
    result = es.search(index="sensor_data", body=query)
    sensors = [bucket["key"] for bucket in result["aggregations"]["sensor_ids"]["buckets"]]
    return {"sensors": sensors}

@app.get("/quartiers")
def get_quartiers():
    """Récupère la liste de tous les quartiers disponibles"""
    query = {
        "size": 0,
        "aggs": {
            "quartiers": {
                "terms": {
                    "field": "quartier",
                    "size": 100
                }
            }
        }
    }
    
    result = es.search(index="sensor_data", body=query)
    quartiers = [bucket["key"] for bucket in result["aggregations"]["quartiers"]["buckets"]]
    return {"quartiers": quartiers}

@app.get("/data")
def get_data(
    sensor_id: Optional[str] = None,
    sensor_type: Optional[str] = None,
    quartier: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000)
):
    """
    Récupère les données des capteurs avec filtres optionnels
    """
    query = {"query": {"bool": {"must": []}}}
    
    if sensor_id:
        query["query"]["bool"]["must"].append({"term": {"sensor_id": sensor_id}})
    
    if sensor_type:
        query["query"]["bool"]["must"].append({"term": {"sensor_type": sensor_type}})
        
    if quartier:
        query["query"]["bool"]["must"].append({"term": {"quartier": quartier}})
        
    if start_date:
        query["query"]["bool"]["must"].append({
            "range": {"timestamp": {"gte": start_date}}
        })
        
    if end_date:
        query["query"]["bool"]["must"].append({
            "range": {"timestamp": {"lte": end_date}}
        })
    
    if not query["query"]["bool"]["must"]:
        query["query"] = {"match_all": {}}
        
    query["size"] = limit
    query["sort"] = [{"timestamp": {"order": "desc"}}]
    
    try:
        result = es.search(index="sensor_data", body=query)
        data = [hit["_source"] for hit in result["hits"]["hits"]]
        return {"count": len(data), "data": data}
    except Exception as e:
        return {"error": str(e)}

@app.get("/aggregate")
def aggregate_data(
    sensor_type: str,
    quartier: Optional[str] = None,
    aggregation: str = Query("avg", regex="^(avg|min|max|sum)$"),
    interval: str = Query("hour", regex="^(minute|hour|day|week|month)$"),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """
    Agrège les données des capteurs selon différentes dimensions
    """
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"sensor_type": sensor_type}}
                ]
            }
        },
        "aggs": {
            "time_buckets": {
                "date_histogram": {
                    "field": "timestamp",
                    "calendar_interval": interval
                },
                "aggs": {
                    "value_agg": {
                        aggregation: {"field": "value"}
                    }
                }
            }
        },
        "size": 0
    }
    
    if quartier:
        query["query"]["bool"]["must"].append({"term": {"quartier": quartier}})
        
    if start_date:
        query["query"]["bool"]["must"].append({
            "range": {"timestamp": {"gte": start_date}}
        })
        
    if end_date:
        query["query"]["bool"]["must"].append({
            "range": {"timestamp": {"lte": end_date}}
        })
    
    try:
        result = es.search(index="sensor_data", body=query)
        data = []
        for bucket in result["aggregations"]["time_buckets"]["buckets"]:
            data.append({
                "timestamp": bucket["key_as_string"],
                "value": bucket["value_agg"]["value"]
            })
        return {"aggregation": aggregation, "interval": interval, "data": data}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)