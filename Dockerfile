# Offizielles Flink-Image als Basis
FROM flink:1.18-java11

# Als root arbeiten, um Pakete zu installieren
USER root

# Python und notwendige Tools installieren.
# JDK und Compiler braucht nur der Build von pemja (PyFlink-Abhängigkeit), für das es
# auf arm64 (z. B. Apple Silicon) kein fertiges Paket gibt.
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev build-essential openjdk-11-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

# Symlink erstellen, damit Flink 'python' statt 'python3' findet
RUN ln -s /usr/bin/python3 /usr/bin/python

# PyFlink installieren (passend zur Flink-Version!); JAVA_HOME zeigt dafür auf das JDK
RUN JAVA_HOME=/usr/lib/jvm/java-11-openjdk-$(dpkg --print-architecture) \
    pip3 install --no-cache-dir apache-flink==1.18.1

# Kafka-Connector und Python-Unterstützung in den Klassenpfad von Flink legen
RUN wget -q -P /opt/flink/lib \
        https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar && \
    cp /opt/flink/opt/flink-python-*.jar /opt/flink/lib/

# Zurück zum flink-User für die Sicherheit
USER flink