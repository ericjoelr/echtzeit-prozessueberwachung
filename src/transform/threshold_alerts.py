from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# Flink-Umgebung im Streaming-Modus
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(environment_settings=settings)

# Parameter
BOOTSTRAP_SERVER = "kafka:29092"
READ_TOPIC = "sensor-raw"
WRITE_TOPIC = "temperature-alerts"
SCAN_STARTUP_MODE = "earliest-offset"

EVENT_WATERMARK_SECONDS = 1
WINDOW_SIZE_MINUTES = 5

STD_MULTIPLIKATOR = 3
LEAST_VALUES_FOR_STD = 3000  # 5 min bei einem Event alle 100 ms


# Quelltabelle definieren
t_env.execute_sql(f"""
    CREATE TABLE kafka_source (
        `Time` STRING,
        `MachineId` STRING,
        `MachineName` STRING,
        `ProcessData` ARRAY<ROW<`Name` STRING, `Value` STRING, `Type` STRING, `Time` STRING, `Unit` STRING>>,
        event_time AS TO_TIMESTAMP(`Time`, 'yyyy-MM-dd''T''HH:mm:ss.SSS'),
        WATERMARK FOR event_time AS event_time - INTERVAL '{EVENT_WATERMARK_SECONDS}' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{READ_TOPIC}',
        'properties.bootstrap.servers' = '{BOOTSTRAP_SERVER}',
        'properties.group.id' = 'threshold-alerts',
        'scan.startup.mode' = '{SCAN_STARTUP_MODE}',
        'format' = 'json'
    )
""")

# Berechnen der Mittelwerte und Standardabweichungen in einem sliding Window
t_env.execute_sql(f"""
    CREATE TEMPORARY VIEW enriched AS
    SELECT
            MachineId,
            event_time,
            pd.`Name` AS process_name,
            CAST(pd.`Value` AS DOUBLE) AS current_value,
            AVG(CAST(pd.`Value` AS DOUBLE)) OVER w AS mean,
            STDDEV_POP(CAST(pd.`Value` AS DOUBLE)) OVER w AS stddev,
            COUNT(*) OVER w AS cnt
        FROM kafka_source
        CROSS JOIN UNNEST(ProcessData) AS pd
        WHERE pd.`Name` IN ('fHaertetemperatur (Haerten)', 'fAnlasstemperatur (Anlassen)')
        WINDOW w AS (
            PARTITION BY MachineId, pd.`Name`
            ORDER BY event_time
            RANGE BETWEEN INTERVAL '{WINDOW_SIZE_MINUTES}' MINUTE PRECEDING AND CURRENT ROW
        )
""")

# Filtern der Alarme nach Six Sigma und der Mindestanzahl an Zeilen für die STD
t_env.execute_sql(f"""
    CREATE TEMPORARY VIEW alarms AS
    SELECT
        event_time AS alarm_time,
        MachineId,
        process_name,
        current_value,
        mean,
        stddev,
        mean + {STD_MULTIPLIKATOR} * stddev AS upper_limit,
        mean - {STD_MULTIPLIKATOR} * stddev AS lower_limit
    FROM enriched
    WHERE
        stddev IS NOT NULL
        AND cnt >= {LEAST_VALUES_FOR_STD}
        AND (current_value > mean + {STD_MULTIPLIKATOR} * stddev OR current_value < mean - {STD_MULTIPLIKATOR} * stddev)
""")

# Senke für Alarme
t_env.execute_sql(f"""
    CREATE TABLE kafka_sink (
        alarm_time      TIMESTAMP(3),
        MachineId       STRING,
        process_name    STRING,
        current_value   DOUBLE,
        mean            DOUBLE,
        stddev          DOUBLE,
        upper_limit     DOUBLE,
        lower_limit     DOUBLE
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{WRITE_TOPIC}',
        'properties.bootstrap.servers' = '{BOOTSTRAP_SERVER}',
        'format' = 'json'
    )
""")

# Alarme in Kafka schreiben
# Ohne .wait(): `flink run -d` schickt den Job ab und kehrt sofort zurück
t_env.execute_sql("""
    INSERT INTO kafka_sink
    SELECT alarm_time, MachineId, process_name, current_value, mean, stddev,
           upper_limit, lower_limit
    FROM alarms
""")
