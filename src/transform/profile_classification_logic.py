"""Fachlogik der Profilerkennung (Job 3), ohne Flink.

Ein Profil besteht aus den Abschnitten niedrig -> steigend -> hoch -> fallend.
Pro Messwert wird der Durchmesser geglättet, einem Abschnitt zugeordnet und bei
jedem Abschnittswechsel die Länge des beendeten Abschnitts festgehalten. Nach
fallend -> niedrig ist ein Profil fertig und wird gegen die Soll-Längen geprüft.

Der Flink-Job (profile_detection.py) hält pro Maschine einen Zustand und ruft für
jeden Messwert update() auf.
"""

LOW_DIAMETER = 12.0  # mm
HIGH_DIAMETER = 17.0  # mm
DIAMETER_TOLERANCE = 0.3  # mm, Abstand zum Plateau, der noch als niedrig/hoch gilt
TREND_THRESHOLD = 0.1  # mm Änderung im Fenster, ab der eine Rampe erkannt wird
WINDOW = 10  # Messwerte für Median und Trend

# Soll-Länge je Abschnitt in mm und erlaubte Abweichung. Die Werte sind gemessene
# Längen (Median über die erzeugten Daten), keine Konstruktionsmaße: Die Bänder um
# 12 und 17 mm zählen Anfang und Ende jeder Rampe zum Plateau.
TARGET_LENGTHS = {"niedrig": 952, "steigend": 210, "hoch": 5710, "fallend": 209}
LENGTH_TOLERANCE = 30.0

NEXT_SECTION = {"niedrig": "steigend", "steigend": "hoch", "hoch": "fallend", "fallend": "niedrig"}


def new_state():
    """Leerer Zustand für eine Maschine."""
    return {
        "raw": [],  # letzte Rohwerte des Durchmessers
        "smooth": [],  # letzte Medianwerte
        "last_len": None,  # Drahtlänge des vorherigen Messwerts
        "profile_count": 0,  # zuletzt vergebene Profil-ID
        "profile": None,  # offenes Profil
    }


def classify_section(smooth):
    """Ordnet die geglätteten Durchmesser einem Abschnitt zu."""
    latest = smooth[-1]
    if abs(latest - HIGH_DIAMETER) <= DIAMETER_TOLERANCE:
        return "hoch"
    if abs(latest - LOW_DIAMETER) <= DIAMETER_TOLERANCE:
        return "niedrig"
    if LOW_DIAMETER < latest < HIGH_DIAMETER:
        trend = smooth[-1] - smooth[0]
        if trend > TREND_THRESHOLD:
            return "steigend"
        if trend < -TREND_THRESHOLD:
            return "fallend"
    return "unbekannt"


def update(state, drahtlaenge, durchmesser):
    """Verarbeitet einen Messwert.

    Verändert den Zustand und gibt ihn zurück, dazu das fertige Profil oder None.
    """
    last_len = state["last_len"] if state["last_len"] is not None else drahtlaenge
    state["last_len"] = drahtlaenge

    # Glätten: Median der letzten Rohwerte, Trend über die letzten Medianwerte
    state["raw"] = (state["raw"] + [durchmesser])[-WINDOW:]
    median = sorted(state["raw"])[len(state["raw"]) // 2]
    state["smooth"] = (state["smooth"] + [median])[-WINDOW:]
    if len(state["smooth"]) < 2:
        return state, None

    section = classify_section(state["smooth"])
    profile = state["profile"]
    if section == "unbekannt" or (profile is not None and section == profile["section"]):
        return state, None

    # Kein offenes Profil: ein neues beginnt nur mit einem niedrigen Abschnitt
    if profile is None:
        if section == "niedrig":
            _start_profile(state, drahtlaenge)
        return state, None

    # Unerwarteter Wechsel: Profil verwerfen
    if section != NEXT_SECTION[profile["section"]]:
        state["profile"] = None
        if section == "niedrig":
            _start_profile(state, drahtlaenge)
        return state, None

    # Erwarteter Wechsel: Länge des beendeten Abschnitts festhalten
    profile["lengths"][profile["section"]] = last_len - profile["section_start"]
    profile["section"] = section
    profile["section_start"] = drahtlaenge
    if section != "niedrig":
        return state, None

    # fallend -> niedrig: Profil fertig, nächstes beginnt
    result = _finish_profile(profile, end=last_len)
    _start_profile(state, drahtlaenge)
    return state, result


def _start_profile(state, drahtlaenge):
    state["profile_count"] += 1
    state["profile"] = {
        "id": state["profile_count"],
        "start": drahtlaenge,
        "section": "niedrig",
        "section_start": drahtlaenge,
        "lengths": {},
    }


def _finish_profile(profile, end):
    lengths = profile["lengths"]
    io = all(
        abs(lengths[name] - target) <= LENGTH_TOLERANCE for name, target in TARGET_LENGTHS.items()
    )
    # Feldnamen wie im Dashboard: L2 = niedrig, L3 = steigend, L1 = hoch, L4 = fallend
    return {
        "profile_id": profile["id"],
        "start_drahtlaenge": profile["start"],
        "end_drahtlaenge": end,
        "L2": lengths["niedrig"],
        "L3": lengths["steigend"],
        "L1": lengths["hoch"],
        "L4": lengths["fallend"],
        "iO": io,
    }
