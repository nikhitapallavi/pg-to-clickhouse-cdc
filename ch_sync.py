import signal
import sys
from kafka import KafkaConsumer, KafkaAdminClient
from clickhouse_driver import Client
import json
import psycopg2
from typing import Dict, List
import logging
import re
import decimal
import base64

def shutdown(signum, frame):
    print("Shutting down gracefully...")
    # close kafka consumer
    # flush any pending batch to ClickHouse
    sys.exit(0)

signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler('/var/log/cdc-pipeline/app.log'),
        logging.StreamHandler()   # also show in terminal
    ]
)
logger = logging.getLogger(__name__)

# ✅ CDC metadata column names — used to strip them from incoming 'after' data
CDC_COLUMNS = {'_cdc_operation', '_cdc_timestamp', '_cdc_version'}


class PostgreSQLToClickHouseSync:
    def __init__(self, kafka_brokers: List[str], clickhouse_host: str,
                 pg_config: Dict, clickhouse_user: str = 'default',
                 clickhouse_password: str = ''):

        self.kafka_brokers = kafka_brokers
        self.pg_config = pg_config
        self.table_cache = {}

        # ✅ ClickHouse client with credentials
        self.clickhouse = Client(
            host=clickhouse_host,
            port=9000,
            user=clickhouse_user,
            password=clickhouse_password,
        )

        # Step 1: List ALL topics for debugging
        all_available_topics = self._list_all_topics()

        # Step 2: Auto-detect data and schema topics
        data_topics, schema_topics = self._detect_topics(all_available_topics)

        logger.info(f"Data topics found   : {data_topics}")
        logger.info(f"Schema topics found : {schema_topics}")

        if not data_topics and not schema_topics:
            logger.error("=" * 60)
            logger.error("NO CDC TOPICS FOUND!")
            logger.error("Check Debezium connector status:")
            logger.error("  curl http://localhost:8083/connectors/postgres-testdb-connector/status")
            logger.error("=" * 60)
            raise RuntimeError("No Kafka CDC topics found. Fix Debezium connector first.")

        all_topics = list(set(data_topics + schema_topics))
        logger.info(f"✅ Subscribing to {len(all_topics)} topics: {all_topics}")

        # Step 3: Create consumer
        self.consumer = KafkaConsumer(
            *all_topics,
            bootstrap_servers=kafka_brokers,
            value_deserializer=lambda m: self._safe_deserialize(m),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='clickhouse-sync-group',
            consumer_timeout_ms=-1,
        )

    def _safe_deserialize(self, m):
        try:
            return json.loads(m.decode('utf-8'))
        except Exception as e:
            logger.warning(f"Failed to deserialize message: {e}")
            return {}

    def _list_all_topics(self):
        try:
            admin = KafkaAdminClient(bootstrap_servers=self.kafka_brokers)
            topics = admin.list_topics()
            admin.close()
            logger.info("=" * 60)
            logger.info("ALL KAFKA TOPICS CURRENTLY AVAILABLE:")
            for t in sorted(topics):
                logger.info(f"    {t}")
            logger.info("=" * 60)
            return list(topics)
        except Exception as e:
            logger.error(f"Could not list Kafka topics: {e}")
            return []

    def _detect_topics(self, all_topics: List[str]):
        skip_prefixes = ('__', 'connect-', 'connect_')
        schema_topics = []
        data_topics = []

        for topic in all_topics:
            if any(topic.startswith(p) for p in skip_prefixes):
                continue
            if topic.startswith('schema-changes.') or topic.startswith('schema_changes.'):
                schema_topics.append(topic)
                continue
            parts = topic.split('.')
            if len(parts) >= 2:
                data_topics.append(topic)

        return data_topics, schema_topics

    def get_postgres_schema(self, db_name: str, table_name: str) -> List[Dict]:
        """Get table column schema from PostgreSQL"""
        try:
            conn = psycopg2.connect(
                host=self.pg_config['host'],
                port=self.pg_config['port'],
                database=db_name,
                user=self.pg_config['user'],
                password=self.pg_config['password']
            )
            cursor = conn.cursor()

            # ✅ Also fetch primary key info
            cursor.execute("""
                SELECT
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    c.column_default,
                    CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key
                FROM information_schema.columns c
                LEFT JOIN (
                    SELECT ku.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage ku
                        ON tc.constraint_name = ku.constraint_name
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                      AND ku.table_name = %s
                      AND ku.table_schema = 'public'
                ) pk ON c.column_name = pk.column_name
                WHERE c.table_schema = 'public' AND c.table_name = %s
                ORDER BY c.ordinal_position
            """, (table_name, table_name))

            columns = []
            for row in cursor.fetchall():
                columns.append({
                    'name': row[0],
                    'type': row[1],
                    'nullable': row[2] == 'YES',
                    'default': row[3],
                    'is_primary_key': row[4]
                })

            cursor.close()
            conn.close()
            return columns

        except Exception as e:
            logger.error(f"Failed to get schema for {db_name}.{table_name}: {e}")
            return []

    def pg_to_clickhouse_type(self, pg_type: str, nullable: bool) -> str:
        """Map PostgreSQL data types to ClickHouse data types"""
        type_mapping = {
            'smallint':                    'Int16',
            'integer':                     'Int32',
            'bigint':                      'Int64',
            'real':                        'Float32',
            'double precision':            'Float64',
            'numeric':                     'String',   # Stored as String to avoid precision loss
            'decimal':                     'String',
            'boolean':                     'UInt8',
            'character varying':           'String',
            'varchar':                     'String',
            'text':                        'String',
            'character':                   'String',
            'char':                        'String',
            'date':                        'Int32',    # Debezium sends dates as int (days since epoch)
            'timestamp without time zone': 'Int64',    # Debezium sends as epoch ms
            'timestamp with time zone':    'Int64',
            'time without time zone':      'String',
            'json':                        'String',
            'jsonb':                       'String',
            'uuid':                        'String',
            'bytea':                       'String',
            'array':                       'String',
            'integer[]':                   'String',
            'text[]':                      'String',
            'money':                       'String',
        }
        base_type = type_mapping.get(pg_type.lower(), 'String')
        if nullable:
            return f'Nullable({base_type})'
        return base_type

    def create_clickhouse_database(self, db_name: str):
        safe_db_name = re.sub(r'[^a-zA-Z0-9_]', '_', db_name)
        try:
            self.clickhouse.execute(f'CREATE DATABASE IF NOT EXISTS `{safe_db_name}`')
            logger.info(f"✅ Database `{safe_db_name}` ready in ClickHouse")
        except Exception as e:
            logger.error(f"Error creating database {safe_db_name}: {e}")

    def create_clickhouse_table(self, db_name: str, table_name: str,
                                columns: List[Dict]) -> bool:
        safe_db_name    = re.sub(r'[^a-zA-Z0-9_]', '_', db_name)
        safe_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)

        column_defs  = []
        primary_keys = []

        for col in columns:
            col_name = col['name']
            col_type = self.pg_to_clickhouse_type(col['type'], col['nullable'])
            column_defs.append(f"`{col_name}` {col_type}")

            # ✅ Use actual primary key from PostgreSQL, fallback to 'id' column
            if col.get('is_primary_key'):
                primary_keys.append(col_name)
            elif 'id' in col_name.lower() and not col['nullable']:
                if col_name not in primary_keys:
                    primary_keys.append(col_name)

        # ✅ CDC metadata columns added to table definition
        column_defs.extend([
            "`_cdc_operation` String",
            "`_cdc_timestamp` Int64",
            "`_cdc_version`   UInt64"
        ])

        if not primary_keys:
            primary_keys = [columns[0]['name']]

        order_by = ', '.join([f'`{pk}`' for pk in primary_keys])

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{safe_db_name}`.`{safe_table_name}` (
            {', '.join(column_defs)}
        ) ENGINE = ReplacingMergeTree(`_cdc_version`)
        ORDER BY ({order_by})
        """

        try:
            self.clickhouse.execute(create_sql)
            logger.info(f"✅ Table `{safe_db_name}`.`{safe_table_name}` created/verified")
            return True
        except Exception as e:
            logger.error(f"Error creating table {safe_db_name}.{safe_table_name}: {e}")
            return False

    def verify_table_has_cdc_columns(self, db_name: str, table_name: str) -> bool:
        """
        ✅ Check if existing ClickHouse table has CDC columns.
        If not, drop and recreate it so CDC columns are included.
        """
        safe_db_name    = re.sub(r'[^a-zA-Z0-9_]', '_', db_name)
        safe_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
        try:
            result = self.clickhouse.execute(
                f"SELECT name FROM system.columns WHERE database='{safe_db_name}' AND table='{safe_table_name}'"
            )
            existing_cols = {row[0] for row in result}

            if not CDC_COLUMNS.issubset(existing_cols):
                logger.warning(
                    f"⚠️  Table `{safe_db_name}`.`{safe_table_name}` is missing CDC columns. "
                    f"Dropping and recreating..."
                )
                self.clickhouse.execute(
                    f"DROP TABLE IF EXISTS `{safe_db_name}`.`{safe_table_name}`"
                )
                # Clear cache so it gets recreated
                self.table_cache.pop(f"{db_name}.{table_name}", None)
                return False
            return True
        except Exception as e:
            logger.error(f"Error verifying table columns: {e}")
            return False

    def ensure_table_exists(self, db_name: str, table_name: str) -> bool:
        cache_key = f"{db_name}.{table_name}"

        if cache_key in self.table_cache:
            return True

        self.create_clickhouse_database(db_name)

        # ✅ Check if table exists but is missing CDC columns — drop & recreate if so
        self.verify_table_has_cdc_columns(db_name, table_name)

        columns = self.get_postgres_schema(db_name, table_name)
        if not columns:
            logger.error(f"No schema found in PostgreSQL for {db_name}.{table_name}")
            return False

        if self.create_clickhouse_table(db_name, table_name, columns):
            self.table_cache[cache_key] = columns
            return True

        return False

    def _clean_record(self, record: dict) -> dict:
        """
        ✅ Remove any CDC metadata keys that Debezium may have
        included in the 'after' payload from a previous snapshot.
        Also convert special Debezium types to ClickHouse-compatible values.
        """
        cleaned = {}
        for k, v in record.items():
            # Strip CDC metadata keys that shouldn't come from source data
            if k in CDC_COLUMNS:
                continue

            # ✅ Handle Decimal type (Debezium sends as base64 bytes string)
            if isinstance(v, str) and len(v) == 4 and not v.isdigit():
                # Likely a base64-encoded Decimal — convert to string representation
                try:
                    raw = base64.b64decode(v + '==')
                    num = int.from_bytes(raw, byteorder='big', signed=True)
                    cleaned[k] = str(num)
                    continue
                except Exception:
                    cleaned[k] = str(v)
                    continue

            # ✅ Handle Python Decimal
            if isinstance(v, decimal.Decimal):
                cleaned[k] = str(v)
                continue

            # ✅ Handle bytes
            if isinstance(v, bytes):
                cleaned[k] = v.hex()
                continue

            cleaned[k] = v

        return cleaned

    def process_message(self, message):
        """Process one CDC data message from Kafka"""
        try:
            payload = message.value
            if not payload:
                return

            # Handle both wrapped {"payload":{...}} and flat Debezium formats
            data = payload.get('payload', payload)

            source     = data.get('source', {})
            db_name    = source.get('db', '')
            table_name = source.get('table', '')
            operation  = data.get('op', '')

            # Fallback: parse db/table from topic name (e.g. postgres.public.employees)
            if not db_name or not table_name:
                parts = message.topic.split('.')
                if len(parts) >= 3:
                    db_name    = self.pg_config.get('dbname', parts[0])
                    table_name = parts[-1]
                elif len(parts) == 2:
                    db_name    = self.pg_config.get('dbname', parts[0])
                    table_name = parts[1]

            if not db_name or not table_name:
                logger.warning(f"Cannot determine db/table from topic: {message.topic}")
                return

            if not self.ensure_table_exists(db_name, table_name):
                logger.error(f"Failed to ensure table {db_name}.{table_name}")
                return

            safe_db_name    = re.sub(r'[^a-zA-Z0-9_]', '_', db_name)
            safe_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)

            op_map = {'c': 'INSERT', 'u': 'UPDATE', 'd': 'DELETE', 'r': 'READ'}
            operation_type = op_map.get(operation, 'INSERT')

            raw_record = data.get('before' if operation == 'd' else 'after', {})

            if not raw_record:
                logger.warning(f"Empty record for {operation_type} on {db_name}.{table_name}")
                return

            # ✅ Clean the record — strip any stale CDC keys, convert special types
            record = self._clean_record(raw_record)

            # ✅ Now attach fresh CDC metadata
            record['_cdc_operation'] = operation_type
            record['_cdc_timestamp'] = data.get('ts_ms', 0)
            record['_cdc_version']   = data.get('ts_ms', 0)

            cols   = list(record.keys())
            values = [record[col] for col in cols]

            insert_sql = f"""
                INSERT INTO `{safe_db_name}`.`{safe_table_name}`
                ({', '.join([f'`{col}`' for col in cols])})
                VALUES
            """
            self.clickhouse.execute(insert_sql, [values])
            logger.info(f"✅ {operation_type} → {db_name}.{table_name} | id={record.get('id', '?')}")

        except Exception as e:
            logger.error(f"Error processing message from {message.topic}: {e}")

    def handle_schema_change(self, message):
        """Handle DDL schema change events"""
        try:
            payload = message.value
            if not payload:
                return

            data    = payload.get('payload', payload)
            source  = data.get('source', {})
            db_name = source.get('db', '')
            ddl     = data.get('ddl', '')

            if not ddl:
                return

            logger.info(f"Schema change in {db_name}: {ddl[:100]}...")

            if 'CREATE TABLE' in ddl.upper():
                match = re.search(
                    r'CREATE TABLE\s+(?:IF NOT EXISTS\s+)?["`]?(\w+)["`]?',
                    ddl, re.IGNORECASE
                )
                if match:
                    table_name = match.group(1)
                    self.table_cache.pop(f"{db_name}.{table_name}", None)
                    self.ensure_table_exists(db_name, table_name)
                    logger.info(f"✅ Auto-created table for {db_name}.{table_name}")

            elif 'ALTER TABLE' in ddl.upper():
                match = re.search(r'ALTER TABLE\s+["`]?(\w+)["`]?', ddl, re.IGNORECASE)
                if match:
                    table_name = match.group(1)
                    self.table_cache.pop(f"{db_name}.{table_name}", None)
                    logger.warning(f"⚠️  ALTER TABLE on {db_name}.{table_name} — cache cleared.")

        except Exception as e:
            logger.error(f"Error handling schema change: {e}")

    def start(self):
        """Main loop"""
        logger.info("🚀 CDC Sync started. Listening for PostgreSQL changes...")
        logger.info("Press Ctrl+C to stop.\n")

        try:
            for message in self.consumer:
                topic = message.topic
                if 'schema-changes' in topic or 'schema_changes' in topic:
                    self.handle_schema_change(message)
                else:
                    self.process_message(message)

        except KeyboardInterrupt:
            logger.info("🛑 Stopping CDC sync...")
        except Exception as e:
            logger.error(f"Consumer fatal error: {e}")
        finally:
            self.consumer.close()
            logger.info("✅ Consumer closed cleanly.")


# ─────────────────────────────────────────────────────────────────
# ✅ CONFIGURATION — Update the values marked with  ←
# ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":

    kafka_brokers = ['localhost:9092']

    clickhouse_host     = 'localhost'
    clickhouse_user     = 'default'
    clickhouse_password = 'nikki17'               # ← ClickHouse password ('' if none)

    pg_config = {
        'host'    : 'localhost',
        'port'    : 5432,
        'dbname'  : 'testdb',              # ← Your PostgreSQL database name
        'user'    : 'debezium_user',
        'password': 'nikki17'        # ← Your PostgreSQL password
    }

    syncer = PostgreSQLToClickHouseSync(
        kafka_brokers       = kafka_brokers,
        clickhouse_host     = clickhouse_host,
        pg_config           = pg_config,
        clickhouse_user     = clickhouse_user,
        clickhouse_password = clickhouse_password,
    )
    syncer.start()
