import os, json, time
from datetime import datetime, timezone
from kafka import KafkaConsumer
import requests
import math

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9093")
TOPIC = os.getenv("TOPIC", "bloodpressure")
ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")
ES_INDEX = os.getenv("ES_INDEX", "bp-anomalies")
ARCHIVE_DIR = os.getenv("ARCHIVE_DIR", "/archive")

def classify_bp(sysv: int, diav: int):
    # Tableau (type AHA, aligné à ton screenshot)
    if sysv > 180 or diav > 120:
        return "HYPERTENSIVE_CRISIS", "hypertension_crisis"
    if sysv >= 140 or diav >= 90:
        return "HYPERTENSION_STAGE_2", "hypertension"
    if (130 <= sysv <= 139) or (80 <= diav <= 89):
        return "HYPERTENSION_STAGE_1", "hypertension"
    if 120 <= sysv <= 129 and diav < 80:
        return "ELEVATED", "elevated"
    if sysv < 120 and diav < 80:
        return "NORMAL", None
    # sinon: cas borderline
    return "OTHER", "check"

def sigmoid(x: float) -> float:
    # numerically stable enough for our small x range
    return 1.0 / (1.0 + math.exp(-x))


def ml_risk_score(sysv: int, diav: int) -> float:
    """Return a probabilistic risk score in [0, 1].

    This is a lightweight logistic model using systolic/diastolic as features.
    It complements the deterministic clinical thresholds.
    """
    # Center around common clinical thresholds (roughly 130/80)
    # Coefficients are chosen to increase risk as pressures rise.
    x = 0.05 * (sysv - 130) + 0.08 * (diav - 80)
    return float(sigmoid(x))

def extract_sys_dia(obs: dict):
    sysv = None
    diav = None
    for comp in obs.get("component", []):
        code = comp.get("code", {}).get("coding", [{}])[0].get("code")
        val = comp.get("valueQuantity", {}).get("value")
        if code == "8480-6":
            sysv = int(val)
        if code == "8462-4":
            diav = int(val)
    return sysv, diav

def archive_normal(obs: dict):
    ts = datetime.now(timezone.utc).strftime("%Y%m%d")
    path = os.path.join(ARCHIVE_DIR, f"normal_{ts}.jsonl")
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obs) + "\n")

def index_anomaly(obs: dict, sysv: int, diav: int, category: str, anomaly_type: str | None):
    patient_ref = obs.get("subject", {}).get("reference", "Patient/unknown")
    patient_id = patient_ref.split("/")[-1]

    ml_risk = ml_risk_score(sysv, diav)
    ml_pred = int(ml_risk >= 0.5)

    doc = {
        "@timestamp": datetime.now(timezone.utc).isoformat(),
        "patient_id": patient_id,
        "systolic": sysv,
        "diastolic": diav,
        "category": category,
        "anomaly_type": anomaly_type or "unknown",
        "ml_risk": ml_risk,
        "ml_pred": ml_pred,
        "fhir": obs
    }

    r = requests.post(f"{ES_URL}/{ES_INDEX}/_doc", json=doc, timeout=10)
    r.raise_for_status()

consumer = None
for i in range(10):
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="bp-consumer",
        )
        print("[CONSUMER] Connected to Kafka")
        break
    except Exception as e:
        print(f"[CONSUMER] Kafka not ready ({e}), retry {i+1}/10...")
        time.sleep(3)

if consumer is None:
    raise RuntimeError("Kafka not available after retries")

print("[CONSUMER] started")
for msg in consumer:
    obs = msg.value
    sysv, diav = extract_sys_dia(obs)
    if sysv is None or diav is None:
        print("[CONSUMER] missing sys/dia, skipping")
        continue

    category, anomaly_type = classify_bp(sysv, diav)

    if category == "NORMAL":
        archive_normal(obs)
        print(f"[CONSUMER] NORMAL archived sys={sysv} dia={diav}")
    else:
        try:
            index_anomaly(obs, sysv, diav, category, anomaly_type)
            print(f"[CONSUMER] ANOMALY indexed {category} sys={sysv} dia={diav}")
        except Exception as e:
            print(f"[CONSUMER] failed to index: {e}")
            time.sleep(2)