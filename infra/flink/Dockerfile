FROM flink:1.17.2-scala_2.12-java11

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip curl && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir apache-flink==1.17.2 protobuf cloudpickle


RUN curl -L -o /opt/flink/lib/flink-sql-connector-kafka-1.17.2.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.2/flink-sql-connector-kafka-1.17.2.jar

RUN curl -L -o /opt/flink/lib/flink-avro-1.17.2.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-avro/1.17.2/flink-avro-1.17.2.jar

RUN curl -L -o /opt/flink/lib/avro-1.11.1.jar \
  https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.1/avro-1.11.1.jar


RUN curl -L -o /opt/flink/lib/jackson-core-2.14.1.jar \
  https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.14.1/jackson-core-2.14.1.jar

RUN curl -L -o /opt/flink/lib/jackson-databind-2.14.1.jar \
  https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.14.1/jackson-databind-2.14.1.jar

RUN curl -L -o /opt/flink/lib/jackson-annotations-2.14.1.jar \
  https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.14.1/jackson-annotations-2.14.1.jar


RUN curl -L -o /opt/flink/lib/commons-compress-1.21.jar \
  https://repo1.maven.org/maven2/org/apache/commons/commons-compress/1.21/commons-compress-1.21.jar


RUN curl -L -o /opt/flink/lib/slf4j-api-1.7.36.jar \
  https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar

COPY start-flink.sh /start-flink.sh
RUN chmod +x /start-flink.sh

ENV PYFLINK_CLIENT_EXECUTABLE=/usr/bin/python3
ENV PYFLINK_PYTHON_EXECUTABLE=/usr/bin/python3

USER flink
ENTRYPOINT ["/start-flink.sh"]