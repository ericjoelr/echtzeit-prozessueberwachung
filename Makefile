.PHONY: up data run test lint down

DATA_FILE = data/process_data.json
FLINK_RUN = docker compose exec -T flink-jobmanager flink run -d -py /opt/flink/jobs
KAFKA_TOPICS = docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092
# Ergebnis-Topics vorab anlegen, sonst meldet das Dashboard beim Start ein fehlendes Topic
RESULT_TOPICS = temperature-alerts correlation profiles profiles-nio

# Kafka und Flink starten, Topics anlegen und die drei Jobs abschicken
up:
	docker compose up --build -d
	@echo "Warte auf Kafka ..."
	@until $(KAFKA_TOPICS) --create --if-not-exists --topic sensor-raw >/dev/null 2>&1; do sleep 2; done
	@for topic in $(RESULT_TOPICS); do \
		$(KAFKA_TOPICS) --create --if-not-exists --topic $$topic >/dev/null; \
	done
	@echo "Warte auf Flink ..."
	@until docker compose exec -T flink-jobmanager flink list >/dev/null 2>&1; do sleep 2; done
	$(FLINK_RUN)/threshold_alerts.py
	$(FLINK_RUN)/correlation.py
	$(FLINK_RUN)/profile_detection.py -pyfs /opt/flink/jobs/profile_classification_logic.py
	@echo "Flink-UI: http://localhost:8081"

# Synthetische Sensordaten erzeugen (nur, wenn sie noch fehlen)
data: $(DATA_FILE)

$(DATA_FILE):
	uv run python src/ingestion/generate_data.py

# Daten mit 10-facher Geschwindigkeit einspielen und das Dashboard starten; Strg+C beendet beides
run: $(DATA_FILE)
	@trap 'kill 0' EXIT; \
	uv run python src/ingestion/producer.py --speed 10 & \
	uv run python src/serving/dashboard.py

test:
	uv run pytest

lint:
	uv run ruff check .
	uv run ruff format --check .

down:
	docker compose down
