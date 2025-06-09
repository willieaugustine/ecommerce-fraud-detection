#!/bin/bash
docker exec -it infrastructure_kafka_1 kafka-topics --create \
  --topic ecommerce-transactions \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

docker exec -it infrastructure_kafka_1 kafka-topics --create \
  --topic transaction-alerts \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
