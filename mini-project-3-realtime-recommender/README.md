# Mini Project 3 - Real-Time Recommendation System

This project combines Spark ALS, Kafka, and Spark Structured Streaming.

## Components

- Batch ALS recommendation model
- Kafka producer
- Kafka topic with 2 partitions
- Spark Structured Streaming consumer
- 30-second window analytics
- 10-second sliding window
- Trending score
- Alerts

## Run Order

```bash
docker compose up -d

bash mini-project-3-realtime-recommender/scripts/create_topic.sh

cd mini-project-3-realtime-recommender
source ../.venv/bin/activate

python src/batch_ml/generate_sample_ratings.py

spark-submit src/batch_ml/train_als.py

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  src/streaming/streaming_recommender.py

python src/producer/kafka_producer.py

Create Docker Compose:

```bash
cat > docker-compose.yml <<'EOF'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
