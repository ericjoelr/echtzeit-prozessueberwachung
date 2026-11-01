"""Spielt die Sensordaten aus data/process_data.json zeitgetreu nach Kafka ab.

Aufruf: uv run python src/ingestion/producer.py [--speed 10]
"""

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

from confluent_kafka import Producer

DATA_FILE = Path("data/process_data.json")
TOPIC = "sensor-raw"
BOOTSTRAP_SERVERS = "localhost:9092"


def report_error(err, msg):
    if err is not None:
        print(f"Zustellung fehlgeschlagen: {err}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="Abspielfaktor: 1 = Echtzeit, 10 = zehnmal schneller, 0 = ohne Pause",
    )
    args = parser.parse_args()

    if not DATA_FILE.exists():
        sys.exit(f"{DATA_FILE} fehlt. Zuerst die Daten erzeugen: make data")

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    print(f"Sende {DATA_FILE} an Topic '{TOPIC}' (Faktor {args.speed}) ...")

    start = time.monotonic()
    first_event_time = None
    with DATA_FILE.open() as f:
        for count, line in enumerate(f, start=1):
            record = json.loads(line)
            event_time = datetime.fromisoformat(record["Time"])
            first_event_time = first_event_time or event_time

            # Warten, bis das Event im (beschleunigten) Zeitverlauf fällig ist
            if args.speed > 0:
                due = (event_time - first_event_time).total_seconds() / args.speed
                time.sleep(max(0, due - (time.monotonic() - start)))

            # Schlüssel = Maschine: alle Events einer Maschine landen in derselben
            # Partition und bleiben so in ihrer Reihenfolge
            producer.produce(
                TOPIC, key=record["MachineId"], value=line.rstrip("\n"), callback=report_error
            )
            producer.poll(0)

            if count % 1000 == 0:
                print(f"{count} Nachrichten gesendet")

    producer.flush()
    print(f"Fertig: {count} Nachrichten gesendet")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Abgebrochen")
