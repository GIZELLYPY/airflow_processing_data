# Use Apache Airflow with Python 3.10 as the base image
FROM apache/airflow:2.3.3-python3.8

USER root

# Install necessary system packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jdk-headless \
    build-essential \
    libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# Create directories for output and set permissions
RUN mkdir -p /opt/airflow/output/silver/base_cargos && \
    chmod 777 /opt/airflow/output/silver/base_cargos && \
    mkdir -p /opt/airflow/output/golden/ && \
    chmod 777 /opt/airflow/output/golden/

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

# Switch back to the airflow user for running the service
USER airflow

# Install additional Python dependencies
RUN pip install --upgrade pip && \
    pip install -v \
    pyspark==3.0.1 \
    psycopg2-binary==2.9.1 \
    great_expectations \
    cryptography \
    pandas \
    pytz


USER root

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh


ENTRYPOINT ["/entrypoint.sh"]
CMD ["bash", "-c", "(airflow webserver &) && airflow scheduler"]


