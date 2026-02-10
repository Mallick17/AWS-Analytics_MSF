FROM flink:1.18-java11

USER root

ARG FLINK_VERSION=1.18.1

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    curl \
    zip \
  && rm -rf /var/lib/apt/lists/* \
  && ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install --no-cache-dir --no-deps \
    apache-flink==${FLINK_VERSION}

RUN pip3 install --no-cache-dir \
    pyyaml==6.0.3 \
    python-dotenv==1.2.1 \
    pydantic==2.12.5

RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar

RUN mkdir -p /opt/flink/checkpoints && chown -R flink:flink /opt/flink/checkpoints

USER flink
