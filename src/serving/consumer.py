"""Gibt die Nachrichten eines Kafka-Topics fortlaufend aus (zum Nachsehen der Job-Ergebnisse).

Aufruf: uv run python src/serving/consumer.py --topic temperature-alerts
Topics: sensor-raw, temperature-alerts, correlation, profiles, profiles-nio
"""

import argparse
import sys
import uuid

from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "localhost:9092"


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--topic", default="sensor-raw", help="Topic (Standard: sensor-raw)")
    args = parser.parse_args()

    # Eigene Gruppe bei jedem Start, damit das Topic immer von Anfang an gelesen wird
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"consumer-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([args.topic])
    print(f"Lese Topic '{args.topic}' (Strg+C beendet) ...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka-Fehler: {msg.error()}", file=sys.stderr)
                continue
            print(msg.value().decode())
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
