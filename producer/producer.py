import os, json, time, uuid, random
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer
from fhir.resources.observation import Observation

fake = Faker()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9093")
TOPIC = os.getenv("TOPIC", "bloodpressure")
INTERVAL = float(os.getenv("PRODUCE_INTERVAL_SEC", "1"))

producer = None
for i in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        print("[PRODUCER] Connected to Kafka")
        break
    except Exception as e:
        print(f"[PRODUCER] Kafka not ready ({e}), retry {i+1}/10...")
        time.sleep(3)

if producer is None:
    raise RuntimeError("Kafka not available after retries")


def make_bp_observation():
    systolic = int(random.gauss(125, 20))
    diastolic = int(random.gauss(82, 12))
    systolic = max(70, min(220, systolic))
    diastolic = max(40, min(140, diastolic))

    patient_id = str(uuid.uuid4())[:8]

    payload = {
        "resourceType": "Observation",
        "id": str(uuid.uuid4()),
        "status": "final",
        "category": [{
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                "code": "vital-signs",
                "display": "Vital Signs",
            }]
        }],
        "code": {
            "coding": [{
                "system": "http://loinc.org",
                "code": "85354-9",
                "display": "Blood pressure panel with all children optional",
            }],
            "text": "Blood pressure",
        },
        "subject": {"reference": f"Patient/{patient_id}"},
        "effectiveDateTime": datetime.now(timezone.utc).isoformat(),
        "component": [
            {
                "code": {
                    "coding": [{
                        "system": "http://loinc.org",
                        "code": "8480-6",
                        "display": "Systolic blood pressure",
                    }]
                },
                "valueQuantity": {
                    "value": systolic,
                    "unit": "mmHg",
                    "system": "http://unitsofmeasure.org",
                    "code": "mm[Hg]",
                },
            },
            {
                "code": {
                    "coding": [{
                        "system": "http://loinc.org",
                        "code": "8462-4",
                        "display": "Diastolic blood pressure",
                    }]
                },
                "valueQuantity": {
                    "value": diastolic,
                    "unit": "mmHg",
                    "system": "http://unitsofmeasure.org",
                    "code": "mm[Hg]",
                },
            },
        ],
        "extension": [{
            "url": "https://example.org/fhir/StructureDefinition/patient-location",
            "valueString": fake.city(),
        }],
    }

    obs = Observation.model_validate(payload)
    # Ensure all values are JSON-serializable (e.g., datetimes become ISO strings)
    return obs.model_dump(mode="json"), systolic, diastolic, patient_id


while True:
    obs_json, sysv, diav, pid = make_bp_observation()
    producer.send(TOPIC, obs_json)
    producer.flush()
    print(f"[PRODUCER] sent patient={pid} sys={sysv} dia={diav}")
    time.sleep(INTERVAL)