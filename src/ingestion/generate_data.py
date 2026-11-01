"""Erzeugt synthetische Sensordaten einer Draht-Härtelinie als JSON Lines.

Ein Event alle 100 ms mit Drahtlänge, Geschwindigkeit, Durchmesser, Härte- und
Anlasstemperatur. Eingebaute Muster, die die Flink-Jobs finden sollen:
- Profile niedrig -> steigend -> hoch -> fallend, ein Teil mit falscher Abschnittslänge (n.i.O.)
- Härtetemperatur folgt der Geschwindigkeit negativ mit Zeitversatz
- vereinzelte Temperaturspitzen

Aufruf: uv run python src/ingestion/generate_data.py [--minutes 60]
"""

import argparse
import json
import random
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path

OUTPUT_FILE = Path("data/process_data.json")
SEED = 42  # fester Seed: gleiche Daten bei jedem Lauf
START_TIME = datetime(2026, 1, 1, 6, 0, 0)
INTERVAL = timedelta(milliseconds=100)

MACHINE_ID = "M-01"
MACHINE_NAME = "Haertelinie 1"

# Profil: Soll-Länge je Abschnitt in mm
SECTIONS = {"niedrig": 940, "steigend": 245, "hoch": 5680, "fallend": 245}
LOW_DIAMETER = 12.0  # mm
HIGH_DIAMETER = 17.0  # mm
SECTION_NOISE = 8  # normale Streuung einer Abschnittslänge in mm
NIO_RATE = 0.1  # Anteil der Profile mit einem falsch langen Abschnitt
NIO_DEVIATION = (60, 120)  # Abweichung dieses Abschnitts in mm

# Geschwindigkeit in mm/s: schwankt langsam um den Mittelwert
SPEED_MEAN = 71
SPEED_MIN, SPEED_MAX = 62, 79

# Härtetemperatur sinkt, wenn der Draht vor LAG_SECONDS schneller lief
HARDENING_BASE = 990  # °C
HARDENING_PER_SPEED = -2.5  # °C je mm/s über dem Mittelwert
LAG_SECONDS = 70.7
TEMPERING_BASE = 460  # °C

SPIKE_RATE = 1 / 3000  # Wahrscheinlichkeit einer Spitze je Event (~ alle 5 min)
SPIKE_SIZE = {"hardening": 45, "tempering": 12}  # °C


def new_profile(rng):
    """Abschnittslängen eines Profils; bei n.i.O. weicht ein Abschnitt deutlich ab."""
    lengths = {name: length + rng.gauss(0, SECTION_NOISE) for name, length in SECTIONS.items()}
    is_nio = rng.random() < NIO_RATE
    if is_nio:
        name = rng.choice(list(lengths))
        lengths[name] += rng.choice([-1, 1]) * rng.uniform(*NIO_DEVIATION)
    return lengths, is_nio


def diameter_at(position, lengths):
    """Soll-Durchmesser an einer Position (mm ab Profilbeginn)."""
    span = HIGH_DIAMETER - LOW_DIAMETER
    if position < lengths["niedrig"]:
        return LOW_DIAMETER
    position -= lengths["niedrig"]
    if position < lengths["steigend"]:
        return LOW_DIAMETER + span * position / lengths["steigend"]
    position -= lengths["steigend"]
    if position < lengths["hoch"]:
        return HIGH_DIAMETER
    position -= lengths["hoch"]
    return HIGH_DIAMETER - span * position / lengths["fallend"]


def spike(rng, size):
    """Meist 0, selten eine Temperaturspitze nach oben oder unten."""
    return rng.choice([-1, 1]) * size if rng.random() < SPIKE_RATE else 0


def measurement(name, value, unit, time):
    return {"Name": name, "Value": value, "Type": "Floating", "Time": time, "Unit": unit}


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--minutes", type=int, default=60, help="Dauer der Daten (Standard: 60)")
    args = parser.parse_args()

    rng = random.Random(SEED)
    event_count = int(timedelta(minutes=args.minutes) / INTERVAL)
    lag_events = round(LAG_SECONDS / INTERVAL.total_seconds())
    past_speeds = deque([SPEED_MEAN] * lag_events, maxlen=lag_events)

    speed = SPEED_MEAN
    drahtlaenge = 0.0
    position = 0.0  # mm ab Beginn des aktuellen Profils
    lengths, is_nio = new_profile(rng)
    profiles, nio_profiles = 1, int(is_nio)

    OUTPUT_FILE.parent.mkdir(exist_ok=True)
    with OUTPUT_FILE.open("w") as f:
        for i in range(event_count):
            time = (START_TIME + i * INTERVAL).isoformat(timespec="milliseconds")

            # Geschwindigkeit: Zufallsweg, der zum Mittelwert zurückgezogen wird
            speed += 0.005 * (SPEED_MEAN - speed) + rng.gauss(0, 0.4)
            speed = min(max(speed, SPEED_MIN), SPEED_MAX)
            lagged_speed = past_speeds[0]
            past_speeds.append(speed)

            step = speed * INTERVAL.total_seconds()
            drahtlaenge += step
            position += step
            if position >= sum(lengths.values()):
                position -= sum(lengths.values())
                lengths, is_nio = new_profile(rng)
                profiles += 1
                nio_profiles += is_nio

            durchmesser = diameter_at(position, lengths) + rng.gauss(0, 0.03)
            haertetemperatur = (
                HARDENING_BASE
                + HARDENING_PER_SPEED * (lagged_speed - SPEED_MEAN)
                + rng.gauss(0, 4)
                + spike(rng, SPIKE_SIZE["hardening"])
            )
            anlasstemperatur = (
                TEMPERING_BASE + rng.gauss(0, 2) + spike(rng, SPIKE_SIZE["tempering"])
            )

            event = {
                "Time": time,
                "MachineId": MACHINE_ID,
                "MachineName": MACHINE_NAME,
                "TimeInterval": "100ms",
                "ProcessData": [
                    measurement("fDrahtlaenge1 (Drehgeber)", round(drahtlaenge, 3), "mm", time),
                    measurement("fGeschwindigkeit1 (Drehgeber)", round(speed, 1), "mm/s", time),
                    measurement("fDurchmesser1 (Haerten)", round(durchmesser, 2), "mm", time),
                    measurement(
                        "fHaertetemperatur (Haerten)", round(haertetemperatur, 3), "°C", time
                    ),
                    measurement(
                        "fAnlasstemperatur (Anlassen)", round(anlasstemperatur, 1), "°C", time
                    ),
                ],
            }
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

    print(f"{event_count} Events, {profiles} Profile ({nio_profiles} n.i.O.) -> {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
