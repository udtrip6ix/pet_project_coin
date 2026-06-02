FROM apache/airflow:2.10.5

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    openjdk-17-jdk \
    procps \
    curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin


RUN mkdir -p /opt/jars && \
    chown -R airflow:root /opt/jars

ENV SPARK_EXTRA_CLASSPATH=/opt/jars/*

USER airflow

RUN pip install --no-cache-dir --default-timeout=1000 pyspark==3.5.0

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --default-timeout=1000 -r /requirements.txt