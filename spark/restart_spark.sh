#!/bin/bash

docker compose down spark-iceberg

docker build -t my-spark-iceberg .

docker compose up spark-iceberg -d