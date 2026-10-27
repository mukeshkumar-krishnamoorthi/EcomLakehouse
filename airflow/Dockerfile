FROM apache/airflow:3.3.2

USER root

# Install OpenJDK 17 and procps for PySpark execution
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Dynamically resolve JAVA_HOME for both ARM64 (Apple Silicon) and AMD64 (x86_64)
RUN JAVA_PATH=$(dirname $(dirname $(readlink -f $(which java)))) \
    && ln -sfn "${JAVA_PATH}" /usr/lib/jvm/default-java

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install project runtime dependencies
RUN pip install --no-cache-dir \
    "pyspark>=3.5.0" \
    "pyyaml>=6.0" \
    "faker" \
    "psycopg[binary]" \
    "sqlalchemy" \
    "boto3" \
    "pandas" \
    "pyarrow"
