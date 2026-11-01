"""Job 3: Profilerkennung.

Dünne Flink-Hülle um profile_classification_logic: liest die Rohdaten aus Kafka,
hält pro Maschine einen Zustand und schreibt fertige Profile nach Kafka (alle nach
`profiles`, die n.i.O.-Profile zusätzlich nach `profiles-nio`).

Abschicken mit dem Logik-Modul für die Worker:
    flink run -d -py /opt/flink/jobs/profile_detection.py \
        -pyfs /opt/flink/jobs/profile_classification_logic.py
"""

from pyflink.common import Row, Types
from pyflink.datastream import KeyedProcessFunction, RuntimeContext, StreamExecutionEnvironment
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

from profile_classification_logic import new_state, update

BOOTSTRAP_SERVER = "kafka:29092"
READ_TOPIC = "sensor-raw"
PROFILE_TOPIC = "profiles"
PROFILE_NIO_TOPIC = "profiles-nio"
SCAN_STARTUP_MODE = "earliest-offset"

DRAHTLAENGE = "fDrahtlaenge1 (Drehgeber)"
DURCHMESSER = "fDurchmesser1 (Haerten)"

PROFILE_FIELDS = [
    "profile_id",
    "start_drahtlaenge",
    "end_drahtlaenge",
    "L2",
    "L3",
    "L1",
    "L4",
    "iO",
]
PROFILE_TYPES = [Types.INT()] + [Types.DOUBLE()] * 6 + [Types.BOOLEAN()]

# Flink-Umgebung: DataStream API für den Zustand, Table API für Kafka
env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

# Quelle: Rohdaten aus Kafka
t_env.execute_sql(f"""
    CREATE TABLE kafka_source (
        `MachineId` STRING,
        `ProcessData` ARRAY<ROW<`Name` STRING, `Value` STRING>>
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{READ_TOPIC}',
        'properties.bootstrap.servers' = '{BOOTSTRAP_SERVER}',
        'properties.group.id' = 'profile-detection',
        'scan.startup.mode' = '{SCAN_STARTUP_MODE}',
        'format' = 'json'
    )
""")
events = t_env.to_data_stream(t_env.sql_query("SELECT MachineId, ProcessData FROM kafka_source"))


class ProfileDetection(KeyedProcessFunction):
    """Ruft für jeden Messwert die Fachlogik auf; der Zustand liegt pro Maschine in Flink."""

    def open(self, runtime_context: RuntimeContext):
        self.state = runtime_context.get_state(
            ValueStateDescriptor("profile_state", Types.PICKLED_BYTE_ARRAY())
        )

    def process_element(self, event, ctx):
        values = {item.Name: float(item.Value) for item in event.ProcessData}
        state = self.state.value() or new_state()
        state, profile = update(state, values[DRAHTLAENGE], values[DURCHMESSER])
        self.state.update(state)
        if profile is not None:
            yield Row(*(profile[field] for field in PROFILE_FIELDS))


profiles = events.key_by(lambda event: event.MachineId).process(
    ProfileDetection(), output_type=Types.ROW_NAMED(PROFILE_FIELDS, PROFILE_TYPES)
)
t_env.create_temporary_view("profiles", t_env.from_data_stream(profiles))

# Senken: beide Topics mit demselben Schema
for table, topic in [("profile_sink", PROFILE_TOPIC), ("profile_nio_sink", PROFILE_NIO_TOPIC)]:
    t_env.execute_sql(f"""
        CREATE TABLE {table} (
            profile_id INT,
            start_drahtlaenge DOUBLE,
            end_drahtlaenge DOUBLE,
            L2 DOUBLE,
            L3 DOUBLE,
            L1 DOUBLE,
            L4 DOUBLE,
            iO BOOLEAN
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVER}',
            'format' = 'json'
        )
    """)

# Beide Inserts in einem StatementSet: ein einziger Flink-Job liest die Quelle einmal.
# Ohne .wait(), damit `flink run -d` sofort zurückkehrt.
columns = ", ".join(PROFILE_FIELDS)
statements = t_env.create_statement_set()
statements.add_insert_sql(f"INSERT INTO profile_sink SELECT {columns} FROM profiles")
statements.add_insert_sql(
    f"INSERT INTO profile_nio_sink SELECT {columns} FROM profiles WHERE NOT iO"
)
statements.execute()
