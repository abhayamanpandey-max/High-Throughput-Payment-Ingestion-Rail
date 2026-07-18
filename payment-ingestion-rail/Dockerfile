FROM flink:1.18.0-scala_2.12

USER root

# Install Python 3 and pip natively
RUN apt-get update && \
    apt-get install -y python3 python3-pip wget && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Pre-install PyFlink with an aggressive timeout
# Added --break-system-packages to prevent Debian environment blocks
RUN pip3 install --no-cache-dir \
    --default-timeout=5000 \
    -i https://pypi.org/simple \
    apache-flink==1.18.0

# Download the Kafka connector JAR into the default Flink lib folder
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar

# Ensure the flink user owns the downloaded JAR file
RUN chown -R flink:flink /opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar

# Switch back to the default flink user for security runtime compliance
USER flink
