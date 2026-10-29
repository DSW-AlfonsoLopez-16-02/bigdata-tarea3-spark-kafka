import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

TIPOS = [
    ("Noise - Residential", "NYPD"),
    ("Heat/Hot Water", "HPD"),
    ("Illegal Parking", "NYPD"),
    ("Blocked Driveway", "NYPD"),
    ("Street Condition", "DOT"),
    ("Water System", "DEP"),
    ("Noise - Street/Sidewalk", "NYPD"),
    ("Rodent", "DOHMH"),
    ("Sanitation Condition", "DSNY"),
    ("Plumbing", "HPD"),
]
BOROUGHS = ["BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND"]

def generar_solicitud():
    tipo, agencia = random.choices(TIPOS, weights=[20, 18, 14, 10, 9, 7, 7, 6, 5, 4])[0]
    return {
        "unique_key": random.randint(60000000, 69999999),
        "complaint_type": tipo,
        "agency": agencia,
        "borough": random.choice(BOROUGHS),
        "created_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

while True:
    msg = generar_solicitud()
    producer.send("solicitudes_311", value=msg)
    print(f"Enviado: {msg}")
    time.sleep(0.5)
