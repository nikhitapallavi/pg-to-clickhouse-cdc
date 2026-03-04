# PostgreSQL to ClickHouse CDC Pipeline

Real-time data replication from PostgreSQL to ClickHouse 
using Debezium, Apache Kafka, and Python.

## Tech Stack
- PostgreSQL (source)
- Debezium 2.4 (CDC capture)
- Apache Kafka 2.13-3.9.0 (message streaming)
- Python (consumer + transformer)
- ClickHouse (destination)

## Quick Start
1. Start Zookeeper and Kafka
2. Deploy Debezium connector
3. Run `python3 ch_sync.py`

## Author
Nikhita Kunisetty
