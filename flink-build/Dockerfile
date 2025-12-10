FROM flink:1.18.1-java11

# Install Python 3.10 and pip (required for PyFlink)
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python

# Install PyFlink libraries
COPY requirements.txt /tmp/
RUN pip3 install -r /tmp/requirements.txt

# Download Connectors (CORRECTED URLs)
# Kafka Connector for Flink 1.18 (Version 3.1.0-1.18)
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# JDBC Connector for Flink 1.18 (Version 3.1.0-1.18)
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.0-1.18/flink-connector-jdbc-3.1.0-1.18.jar

# Postgres Driver
RUN wget -P /opt/flink/lib/ https://jdbc.postgresql.org/download/postgresql-42.6.0.jar