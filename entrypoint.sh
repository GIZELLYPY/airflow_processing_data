#!/bin/bash

# Ensure all environment variables are set properly
AIRFLOW_DB_HOST=${AIRFLOW_DB_HOST:-postgres}
AIRFLOW_DB_USER=${AIRFLOW_DB_USER:-airflow}
AIRFLOW_DB_PASSWORD=${AIRFLOW_DB_PASSWORD:-airflow}
AIRFLOW_USER=${AIRFLOW_USER:-airflow}
AIRFLOW_FIRSTNAME=${AIRFLOW_FIRSTNAME:-Airflow}
AIRFLOW_LASTNAME=${AIRFLOW_LASTNAME:-User}
AIRFLOW_ROLE=${AIRFLOW_ROLE:-Admin}
AIRFLOW_EMAIL=${AIRFLOW_EMAIL:-airflow@example.com}
AIRFLOW_PASSWORD=${AIRFLOW_PASSWORD:-airflow}

# Wait for PostgreSQL to be ready
until PGPASSWORD=$AIRFLOW_DB_PASSWORD psql -h $AIRFLOW_DB_HOST -U $AIRFLOW_DB_USER -c '\q'; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 1
done

# Initialize the database
airflow db init

# Create default Airflow user
airflow users create \
    --username $AIRFLOW_USER \
    --firstname $AIRFLOW_FIRSTNAME \
    --lastname $AIRFLOW_LASTNAME \
    --role $AIRFLOW_ROLE \
    --email $AIRFLOW_EMAIL \
    -p $AIRFLOW_PASSWORD

# Start the Airflow webserver and scheduler
exec "$@"
