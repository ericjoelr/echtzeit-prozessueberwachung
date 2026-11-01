"""Live-Dashboard: Rohdaten und erkannte Profile aus Kafka.

Zwei Hintergrund-Threads lesen die Topics und legen die Nachrichten in Puffer;
ein Dash-Callback holt sie jede Sekunde ab und zeichnet Diagramme und Tabellen neu.

Aufruf: uv run python src/serving/dashboard.py, danach http://localhost:8050
"""

import json
import logging
import sys
import threading
import uuid
from collections import deque

import dash
import pandas as pd
import plotly.graph_objects as go
from confluent_kafka import Consumer
from dash import Input, Output, State, dash_table, dcc, html
from dash.dash_table.Format import Format, Scheme

REFRESH_RATE = 1000  # Aktualisierungsintervall in Millisekunden
MAX_SHOWN_EVENTS = 2000  # Rohdatenpunkte im Diagramm
VISIBLE_LENGTH = 10000  # sichtbarer Ausschnitt der Drahtlänge in mm

BOOTSTRAP_SERVERS = "localhost:9092"
RAW_TOPIC = "sensor-raw"
PROFILE_TOPIC = "profiles"

MEASUREMENTS = {
    "fDrahtlaenge1 (Drehgeber)": "Drahtlaenge",
    "fDurchmesser1 (Haerten)": "Durchmesser",
    "fHaertetemperatur (Haerten)": "Haertetemperatur",
    "fAnlasstemperatur (Anlassen)": "Anlasstemperatur",
}

# Längen in mm mit einer Nachkommastelle anzeigen
LENGTH = {"type": "numeric", "format": Format(precision=1, scheme=Scheme.fixed)}
PROFILE_COLUMNS = [
    {"name": "Profil-ID", "id": "profile_id"},
    {"name": "Start Länge", "id": "start_drahtlaenge", **LENGTH},
    {"name": "End Länge", "id": "end_drahtlaenge", **LENGTH},
    {"name": "L2", "id": "L2", **LENGTH},
    {"name": "L3", "id": "L3", **LENGTH},
    {"name": "L1", "id": "L1", **LENGTH},
    {"name": "L4", "id": "L4", **LENGTH},
    {"name": "i.O.", "id": "iO"},
]

# Puffer zwischen Kafka-Threads und Callback. deque ist für append/popleft
# threadsicher und verwirft bei vollem Puffer die ältesten Einträge.
raw_buffer = deque(maxlen=10000)
profile_buffer = deque(maxlen=10000)

# Angezeigte Daten; der Callback ergänzt sie bei jedem Durchlauf
shown_rows = deque(maxlen=MAX_SHOWN_EVENTS)
profiles = []


def consume(topic, buffer, offset_reset):
    """Liest ein Topic endlos und legt jede Nachricht als Dict in den Puffer."""
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"dashboard-{uuid.uuid4()}",  # eigene Gruppe je Start
            "auto.offset.reset": offset_reset,
        }
    )
    consumer.subscribe([topic])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Kafka-Fehler auf {topic}: {msg.error()}", file=sys.stderr)
            continue
        buffer.append(json.loads(msg.value()))


def to_row(event):
    """Macht aus einem Rohdaten-Event eine Zeile mit den angezeigten Messwerten."""
    row = {"Time": pd.to_datetime(event["Time"])}
    for item in event["ProcessData"]:
        if item["Name"] in MEASUREMENTS:
            row[MEASUREMENTS[item["Name"]]] = float(item["Value"])
    return row


# Rohdaten nur ab jetzt (Live-Ansicht), Profile von Anfang an, damit keins fehlt
threading.Thread(target=consume, args=(RAW_TOPIC, raw_buffer, "latest"), daemon=True).start()
threading.Thread(
    target=consume, args=(PROFILE_TOPIC, profile_buffer, "earliest"), daemon=True
).start()


app = dash.Dash(__name__)

app.layout = html.Div(
    [
        html.H1("Drahtprofil-Monitoring (LIVE)"),
        html.Div(
            [html.Div(id="stats-info"), html.Button("Reset", id="btn-reset", n_clicks=0)],
            style={
                "margin": "10px",
                "padding": "10px",
                "backgroundColor": "#f0f0f0",
                "borderRadius": "5px",
                "display": "flex",
                "justifyContent": "space-between",
                "alignItems": "center",
            },
        ),
        dcc.Checklist(
            id="temp-checkboxes",
            options=[
                {"label": "Härtetemperatur", "value": "Haertetemperatur"},
                {"label": "Anlasstemperatur", "value": "Anlasstemperatur"},
            ],
            value=["Haertetemperatur", "Anlasstemperatur"],
            inline=True,
            style={"margin": "10px"},
        ),
        html.Div(
            [
                html.Label("Temperatur Y-Achse:"),
                dcc.Input(
                    id="temp-min-input",
                    type="number",
                    placeholder="min (auto)",
                    style={"width": "170px"},
                ),
                dcc.Input(
                    id="temp-max-input",
                    type="number",
                    placeholder="max (auto)",
                    style={"width": "170px"},
                ),
            ],
            style={"margin": "10px", "display": "flex", "gap": "10px", "alignItems": "center"},
        ),
        dcc.Graph(id="live-line-chart"),
        html.Hr(),
        html.Div(
            [
                html.Div(
                    [
                        html.H3("Erkannte Profile"),
                        dash_table.DataTable(
                            id="profile-table",
                            columns=PROFILE_COLUMNS,
                            page_size=10,
                            style_cell={"textAlign": "left"},
                            style_header={"fontWeight": "bold"},
                            style_data_conditional=[
                                {
                                    "if": {"filter_query": '{iO} = "nein"'},
                                    "backgroundColor": "#FFE5E5",
                                }
                            ],
                        ),
                        html.H3("Fehlerhafte Profile", style={"marginTop": "30px"}),
                        dash_table.DataTable(
                            id="faulty-profile-table",
                            columns=PROFILE_COLUMNS[:-1],
                            page_size=5,
                            style_cell={"textAlign": "left"},
                            style_header={"fontWeight": "bold"},
                        ),
                    ],
                    style={"width": "60%", "display": "inline-block", "verticalAlign": "top"},
                ),
                html.Div(
                    [html.H3("Qualitätsverteilung"), dcc.Graph(id="pie-chart")],
                    style={
                        "width": "35%",
                        "display": "inline-block",
                        "verticalAlign": "top",
                        "marginLeft": "2%",
                    },
                ),
            ]
        ),
        dcc.Interval(id="interval", interval=REFRESH_RATE),
    ]
)


@app.callback(
    Output("live-line-chart", "figure"),
    Output("profile-table", "data"),
    Output("faulty-profile-table", "data"),
    Output("pie-chart", "figure"),
    Output("stats-info", "children"),
    Input("interval", "n_intervals"),
    Input("btn-reset", "n_clicks"),
    Input("temp-checkboxes", "value"),
    State("temp-min-input", "value"),
    State("temp-max-input", "value"),
)
def update_dashboard(n_intervals, reset_clicks, temp_visible, temp_min, temp_max):
    if dash.ctx.triggered_id == "btn-reset":
        shown_rows.clear()
        profiles.clear()
    else:
        # Alles abholen, was die Threads seit dem letzten Aufruf gelesen haben
        while raw_buffer:
            shown_rows.append(to_row(raw_buffer.popleft()))
        while profile_buffer:
            profiles.append(profile_buffer.popleft())

    df = pd.DataFrame(list(shown_rows), columns=["Time", *MEASUREMENTS.values()])
    faulty = [p for p in profiles if not p["iO"]]
    table_data = [{**p, "iO": "ja" if p["iO"] else "nein"} for p in profiles[-20:]]

    return (
        create_line_chart(df, temp_visible, temp_min, temp_max),
        table_data,
        faulty,
        create_pie_chart(len(profiles) - len(faulty), len(faulty)),
        stats_text(df, len(profiles), len(faulty)),
    )


def stats_text(df, total, faulty):
    """Kopfzeile: angezeigter Zeitraum und Anzahl der Profile."""
    if df.empty:
        period = "–"
    else:
        start, end = df["Time"].min(), df["Time"].max()
        seconds = int((end - start).total_seconds())
        period = (
            f"{start:%H:%M:%S} – {end:%H:%M:%S} (Dauer: {seconds // 60}:{seconds % 60:02d} Min)"
        )
    return [
        f"Datenpunkte: {len(df)} | Zeitraum: {period} | ",
        html.B(f"Erkannte Profile: {total}"),
        " | ",
        html.B(f"Korrekte Profile: {total - faulty}", style={"color": "green"}),
        " | ",
        html.B(f"Fehlerhafte Profile: {faulty}", style={"color": "red"}),
    ]


def create_pie_chart(io_count, nio_count):
    fig = go.Figure(
        go.Pie(
            labels=["i.O.", "n.i.O."],
            values=[io_count, nio_count],
            marker={"colors": ["green", "red"]},
            textinfo="label+percent",
        )
    )
    fig.update_layout(height=250)
    return fig


def create_line_chart(df, temp_visible, temp_min, temp_max):
    """Durchmesser und Temperaturen über der Drahtlänge, Profilgrenzen als gestrichelte Linien."""
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(x=df["Drahtlaenge"], y=df["Durchmesser"], name="Durchmesser", line_color="blue")
    )
    for column, label, color in [
        ("Haertetemperatur", "Härtetemperatur", "red"),
        ("Anlasstemperatur", "Anlasstemperatur", "orange"),
    ]:
        if column in temp_visible:
            fig.add_trace(
                go.Scatter(
                    x=df["Drahtlaenge"], y=df[column], name=label, line_color=color, yaxis="y2"
                )
            )

    xaxis = {"title": "Drahtlänge (mm)"}
    if not df.empty:
        x_max = df["Drahtlaenge"].max()
        x_min = x_max - VISIBLE_LENGTH
        xaxis["range"] = [x_min, x_max]
        # Start und Ende jedes Profils im sichtbaren Ausschnitt markieren
        for profile in profiles:
            color = "green" if profile["iO"] else "red"
            for x in (profile["start_drahtlaenge"], profile["end_drahtlaenge"]):
                if x_min <= x <= x_max:
                    fig.add_vline(x=x, line={"color": color, "width": 2, "dash": "dash"})

    # Temperaturachse nur fest, wenn beide Grenzen eingegeben sind
    temp_range = [temp_min, temp_max] if temp_min is not None and temp_max is not None else None
    fig.update_layout(
        xaxis=xaxis,
        yaxis={"title": "Durchmesser (mm)", "range": [10, 20]},
        yaxis2={
            "title": "Temperatur (°C)",
            "overlaying": "y",
            "side": "right",
            "range": temp_range,
        },
        hovermode="x unified",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
    )
    return fig


if __name__ == "__main__":
    print(f"Dashboard liest {RAW_TOPIC} und {PROFILE_TOPIC} von {BOOTSTRAP_SERVERS}")
    print("Dashboard verfügbar unter: http://localhost:8050")
    # Kein Log je Anfrage: Der Callback läuft jede Sekunde und würde die Ausgabe des Producers verdecken
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    app.run(host="127.0.0.1", port=8050)
