#!/bin/bash
# register_connectors.sh

DATABASES=("testdb")

for DB in "${DATABASES[@]}"; do
  curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"postgres-$DB-connector\",
    \"config\": {
      \"connector.class\": \"io.debezium.connector.postgresql.PostgresConnector\",
      \"database.hostname\": \"localhost\",
      \"database.port\": \"5432\",
      \"database.user\": \"debezium_user\",
      \"database.password\": \"nikki17\",
      \"database.dbname\": \"$DB\",


      \"topic.prefix\": \"postgres_$DB\",


      \"table.include.list\": \"public.*\",
      \"plugin.name\": \"pgoutput\",
      \"publication.autocreate.mode\": \"filtered\",
      \"slot.name\": \"debezium_$DB\",
      \"snapshot.mode\": \"initial\",
      \"heartbeat.interval.ms\": \"10000\",
      \"schema.history.internal.kafka.bootstrap.servers\": \"localhost:9092\",
      \"schema.history.internal.kafka.topic\": \"schema-changes.$DB\",

      \"transforms\": \"unwrap\",
      \"transforms.unwrap.type\": \"io.debezium.transforms.ExtractNewRecordState\",
      \"transforms.unwrap.drop.tombstones\": \"false\",
      \"transforms.unwrap.delete.handling.mode\": \"rewrite\"
    }
  }"
done
