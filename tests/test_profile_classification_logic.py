from profile_classification_logic import new_state, update

STEP = 7.0  # mm Draht je Messwert, wie bei ~70 mm/s und 100 ms Takt


def profile_points(start, niedrig=940, steigend=245, hoch=5680, fallend=245):
    """Messpunkte (drahtlaenge, durchmesser) eines idealen Profils ab Position `start`.

    Gibt die Punkte und die Position nach dem Profil zurück.
    """
    sections = [
        (niedrig, lambda share: 12.0),
        (steigend, lambda share: 12.0 + 5.0 * share),
        (hoch, lambda share: 17.0),
        (fallend, lambda share: 17.0 - 5.0 * share),
    ]
    points = []
    position = start
    for length, diameter in sections:
        section_start = position
        while position < section_start + length:
            points.append((position, diameter((position - section_start) / length)))
            position += STEP
    return points, position


def line(*profiles):
    """Hängt Profile aneinander und schließt mit dem Beginn eines weiteren ab.

    Jedes Argument ist ein Dict mit abweichenden Abschnittslängen ({} = ideales Profil).
    Ein Profil gilt erst als fertig, wenn danach wieder `niedrig` erkannt wird.
    """
    points, position = [], 0.0
    for lengths in profiles:
        new, position = profile_points(position, **lengths)
        points += new
    tail, _ = profile_points(position, steigend=0, hoch=0, fallend=0)
    return points + tail


def run(points):
    """Schickt alle Messpunkte durch die Logik und sammelt die fertigen Profile."""
    state = new_state()
    profiles = []
    for drahtlaenge, durchmesser in points:
        state, profile = update(state, drahtlaenge, durchmesser)
        if profile is not None:
            profiles.append(profile)
    return profiles


# Das erste Profil jeder Linie dient zum Einschwingen: Die Logik beginnt ein Profil
# erst beim ersten erkannten Abschnitt `niedrig`, daher prüfen die Tests das letzte.


def test_correct_profile_is_io():
    profiles = run(line({}, {}))

    assert profiles[-1]["iO"] is True


def test_section_too_long_is_nio():
    reference = run(line({}, {}))[-1]
    profiles = run(line({}, {"hoch": 5680 + 100}))

    assert profiles[-1]["iO"] is False
    assert profiles[-1]["L1"] - reference["L1"] == 98  # 100 mm, gerastert auf 7-mm-Schritte


def test_unexpected_section_change_discards_profile():
    # Das zweite Profil springt ohne Rampe von niedrig auf hoch
    profiles = run(line({}, {"steigend": 0}, {}))

    assert len(profiles) == 2  # erstes und drittes Profil, das zweite wird verworfen
    assert profiles[-1]["iO"] is True


def test_single_outlier_is_smoothed_by_median():
    first, _ = profile_points(0.0)
    points = line({}, {})
    # Ein Ausreißer auf 17 mm mitten im niedrigen Abschnitt des zweiten Profils
    index = len(first) + 50
    points[index] = (points[index][0], 17.0)

    profiles = run(points)

    assert len(profiles) == 2
    assert profiles[-1]["iO"] is True
