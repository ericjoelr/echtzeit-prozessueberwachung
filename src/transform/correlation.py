from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# Flink-Umgebung im Streaming-Modus
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(environment_settings=settings)

# Parameter
BOOTSTRAP_SERVER = "kafka:29092"
READ_TOPIC = "sensor-raw"
WRITE_TOPIC = "correlation"
SCAN_STARTUP_MODE = "earliest-offset"

EVENT_WATERMARK_SECONDS = 1
WINDOW_SIZE_MINUTES = 2
WINDOW_REFRESH_RATE_SECONDS = 10
CORRELATION_LAG_SECONDS = 70.7  # Versatz zwischen Geschwindigkeit und Härtetemperatur


# Quelle: Rohdaten aus Kafka
t_env.execute_sql(f"""
    CREATE TABLE kafka_source (
        `Time` STRING,
        `MachineId` STRING,
        `ProcessData` ARRAY<ROW<`Name` STRING, `Value` STRING>>,
        event_time AS TO_TIMESTAMP(`Time`, 'yyyy-MM-dd''T''HH:mm:ss.SSS'),
        WATERMARK FOR event_time AS event_time - INTERVAL '{EVENT_WATERMARK_SECONDS}' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{READ_TOPIC}',
        'properties.bootstrap.servers' = '{BOOTSTRAP_SERVER}',
        'properties.group.id' = 'correlation',
        'scan.startup.mode' = '{SCAN_STARTUP_MODE}',
        'format' = 'json'
    )
""")

# Härtetemperatur-Werte
t_env.execute_sql("""
    CREATE TEMPORARY VIEW temp_events AS
    SELECT
        event_time,
        MachineId,
        CAST(pd.`Value` AS DOUBLE) AS h_temp
    FROM kafka_source, UNNEST(ProcessData) AS pd
    WHERE pd.`Name` = 'fHaertetemperatur (Haerten)'
""")

# Geschwindigkeits-Werte
t_env.execute_sql("""
    CREATE TEMPORARY VIEW speed_events AS
    SELECT
        event_time,
        MachineId,
        CAST(pd.`Value` AS DOUBLE) AS geschwindigkeit
    FROM kafka_source, UNNEST(ProcessData) AS pd
    WHERE pd.`Name` = 'fGeschwindigkeit1 (Drehgeber)'
""")

# Jede Temperatur mit der Geschwindigkeit von vor CORRELATION_LAG_SECONDS paaren.
# BETWEEN mit gleichen Grenzen: Flink erkennt so einen Interval Join und kann alte
# Events aus dem State löschen.
t_env.execute_sql(f"""
    CREATE TEMPORARY VIEW paired_events AS
    SELECT
        t.event_time AS ts,
        t.MachineId,
        t.h_temp,
        s.geschwindigkeit AS geschwindigkeit_delayed
    FROM temp_events t
    JOIN speed_events s
        ON t.MachineId = s.MachineId
        AND s.event_time BETWEEN
            t.event_time - INTERVAL '{CORRELATION_LAG_SECONDS}' SECOND
            AND t.event_time - INTERVAL '{CORRELATION_LAG_SECONDS}' SECOND
""")

# Pearson-Korrelation je gleitendem Fenster (HOP); NULLIF verhindert Division durch 0
t_env.execute_sql(f"""
    CREATE TEMPORARY VIEW correlation_result AS
    SELECT
        MachineId,
        HOP_START(ts, INTERVAL '{WINDOW_REFRESH_RATE_SECONDS}' SECOND, INTERVAL '{WINDOW_SIZE_MINUTES}' MINUTE) AS window_start,
        HOP_END(ts, INTERVAL '{WINDOW_REFRESH_RATE_SECONDS}' SECOND, INTERVAL '{WINDOW_SIZE_MINUTES}' MINUTE) AS window_end,
        COUNT(*) AS pair_count,
        (COUNT(*) * SUM(h_temp * geschwindigkeit_delayed) - SUM(h_temp) * SUM(geschwindigkeit_delayed)) /
        NULLIF(
            SQRT(COUNT(*) * SUM(h_temp * h_temp) - SUM(h_temp) * SUM(h_temp)) *
            SQRT(COUNT(*) * SUM(geschwindigkeit_delayed * geschwindigkeit_delayed) - SUM(geschwindigkeit_delayed) * SUM(geschwindigkeit_delayed)),
            0
        ) AS correlation
    FROM paired_events
    WHERE h_temp IS NOT NULL AND geschwindigkeit_delayed IS NOT NULL
    GROUP BY
        MachineId,
        HOP(ts, INTERVAL '{WINDOW_REFRESH_RATE_SECONDS}' SECOND, INTERVAL '{WINDOW_SIZE_MINUTES}' MINUTE)
""")

# Senke für die Korrelationswerte
t_env.execute_sql(f"""
    CREATE TABLE correlation_sink (
        MachineId       STRING,
        window_start    TIMESTAMP(3),
        window_end      TIMESTAMP(3),
        pair_count      BIGINT,
        correlation     DOUBLE
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{WRITE_TOPIC}',
        'properties.bootstrap.servers' = '{BOOTSTRAP_SERVER}',
        'format' = 'json'
    )
""")

# In Kafka schreiben; ohne .wait(), damit `flink run -d` sofort zurückkehrt
t_env.execute_sql("""
    INSERT INTO correlation_sink
    SELECT
        MachineId,
        window_start,
        window_end,
        pair_count,
        correlation
    FROM correlation_result
""")
