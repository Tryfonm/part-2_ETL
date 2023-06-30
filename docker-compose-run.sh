#!/bin/bash

spark_worker_count=$1
docker compose up --scale spark-worker=$spark_worker_count
exit_code=$(docker wait etl-spark-client-1 2>&1)

if [ $exit_code -eq 0 ]; then
  docker compose down
fi
