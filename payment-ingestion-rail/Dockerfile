# ============================================================================
# HIGH-THROUGHPUT PYFLINK STREAMING ENGINE
# Architecture: Flink 1.18 + PyFlink + Kafka Connector
# ============================================================================

FROM flink:1.18.0-scala_2.12-java11

# ============================================================================
# METADATA
# ============================================================================
LABEL maintainer="Abhay Pandey <abhay.pandey@example.com>"
LABEL version="1.0"
LABEL description="PyFlink 1.18 streaming engine for payment transaction processing"

# ============================================================================
# BUILD STAGE: Install dependencies and optimize image
# ============================================================================
USER root

# Install Python 3, pip, and required utilities in single RUN to minimize layers
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-dev \
        build-essential \
        wget \
        curl \
        git \
        ca-certificates \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --upgrade pip setuptools wheel

# Install Python dependencies for PyFlink and data processing
RUN pip3 install --no-cache-dir \
    --default-timeout=60 \
    pydantic==2.5.0 \
    confluent-kafka==2.3.0 \
    pandas==2.1.3 \
    numpy==1.26.2

# Download Kafka connector JAR for Flink SQL
# Using explicit -O flag with error checking for robustness
RUN wget --progress=dot:mega \
    -O /opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar \
    && test -f /opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar \
    || (echo "Failed to download Kafka connector" && exit 1) \
    && chmod 644 /opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar

# Download additional Flink connectors for flexibility
RUN wget --progress=dot:mega \
    -O /opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar

# Ensure flink user owns all libraries
RUN chown -R flink:flink /opt/flink/lib/

# ============================================================================
# HEALTH CHECK
# ============================================================================
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8081 || exit 1

# ============================================================================
# RUNTIME
# ============================================================================
USER flink

# Set environment variables for Flink
ENV FLINK_HOME=/opt/flink
ENV PATH=$FLINK_HOME/bin:$PATH

# Expose Flink ports
EXPOSE 8081 6123 6121

# Default command (will be overridden by docker-compose)
CMD ["jobmanager"]
