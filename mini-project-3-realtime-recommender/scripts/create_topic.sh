#!/bin/bash

docker exec kafka kafka-topics \
  --create \
  --if-not-exists \
  --topic user_interactions \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 1

docker exec kafka kafka-topics \
  --describe \
  --topic user_interactions \
  --bootstrap-server localhost:9092
