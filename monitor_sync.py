from clickhouse_driver import Client
import psycopg2

def check_sync_status():
    # ✅ Update these credentials to match your ClickHouse setup
    ch_client = Client(
        host='localhost',
        port=9000,
        user='default',
        password='nikki17',          # <-- Put your password here, or leave empty if no password
        database='default'
    )
    
    # Get all databases
    databases = ch_client.execute('SHOW DATABASES')
    
    for (db,) in databases:
        if db in ['system', 'default', 'information_schema', 'INFORMATION_SCHEMA']:
            continue
        
        tables = ch_client.execute(f'SHOW TABLES FROM {db}')
        
        for (table,) in tables:
            try:
                count = ch_client.execute(f'SELECT count() FROM {db}.{table}')[0][0]
                latest = ch_client.execute(
                    f'SELECT max(_cdc_timestamp) FROM {db}.{table}'
                )[0][0]
                print(f"{db}.{table}: {count} rows, latest: {latest}")
            except Exception as e:
                print(f"{db}.{table}: Error - {e}")

if __name__ == "__main__":
    check_sync_status()
