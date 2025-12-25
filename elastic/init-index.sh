#!/bin/sh
set -e

echo "Waiting for Elasticsearch..."
until curl -s http://elasticsearch:9200 >/dev/null; do
  sleep 2
done

echo "Creating index template + index..."

curl -s -X PUT "http://elasticsearch:9200/bp-anomalies" \
  -H "Content-Type: application/json" \
  -d '{
    "mappings": {
      "properties": {
        "@timestamp": {"type": "date"},
        "patient_id": {"type": "keyword"},
        "systolic": {"type": "integer"},
        "diastolic": {"type": "integer"},
        "category": {"type": "keyword"},
        "anomaly_type": {"type": "keyword"},
        "fhir": {"type": "object", "enabled": true}
      }
    }
  }' | cat

echo "\nDone."