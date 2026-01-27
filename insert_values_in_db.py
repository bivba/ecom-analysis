import subprocess
import os
import io
import sys
import requests
from urllib.parse import quote
import pyarrow.parquet as pq
import pyarrow.csv as csv

CONTAINER_NAME = 'clickhouse-server'
SQL_DIR = '.\sql'
CLICKHOUSE_HOST = 'http://localhost:8123'
USER = 'default'
PASSWORD = 'default'
DB_NAME = 'db_1'

def check_container():
    print(f'Проверка работоспособности контейнера {CONTAINER_NAME}')
    try:
        subprocess.run(['docker', 'exec', CONTAINER_NAME, 'clickhouse-client', '--query', 'SELECT 1'], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f'{CONTAINER_NAME} готов к работе')
        return True
    except subprocess.CalledProcessError:
        print(f'Контейнер {CONTAINER_NAME} не запустился')
        sys.exit(1)
        return False
    

def run_sql(table_name):
    TABLE_NAME = table_name
    parquet_file = pq.ParquetFile(f'new_tables/{table_name}.parquet')
    print(f'Заполнение таблицы {table_name}')
    query = f"INSERT INTO {TABLE_NAME} FORMAT CSV"
    # params = {
    # 'query': query,
    # 'database': DB_NAME,
    # 'user': USER,
    # 'password': PASSWORD,

    # 'max_insert_block_size': '1000', 
    
    # 'input_format_parquet_max_block_size': '1000',
    
    # 'max_insert_threads': '1',

    # 'low_cardinality_allow_in_native_format': '0',
    
    # 'max_memory_usage': '10000000000' 
    # }
    # url = f"{CLICKHOUSE_HOST}"

    # cmd = [
    #     'docker',
    #     'exec',
    #     '-i',
    #     CONTAINER_NAME,
    #     'clickhouse-client',
    #     '--database', 'db_1',
    #     '--max_insert_block_size', '1000',
    #     '--max_insert_threads', '1',
    #     '--input_format_parquet_max_block_size', '10000',
    #     '--query', f'insert into {table_name} format PARQUET'
    # ]
    url = f"{CLICKHOUSE_HOST}/?query={query}&database=db_1"
    for i in range(parquet_file.num_row_groups):
        # 1. Читаем кусок Parquet
        table = parquet_file.read_row_group(i)
        
        # 2. Конвертируем этот кусок в CSV прямо в памяти (в буфере)
        # use_threads=False снижает нагрузку
        buffer = io.BytesIO()
        csv.write_csv(table, buffer)
        buffer.seek(0)
        try:
            with open(f'new_tables/{table_name}.parquet', 'rb') as f:
                # stream=True важен для отправки больших файлов
                # verify=False можно использовать, если это самоподписанный сертификат
                r = requests.post(url, data=buffer, auth=(USER, PASSWORD), headers={'Content-type': 'text/csv'})
                
                if r.status_code == 200:
                    print(f"Чанк {i+1} (CSV) отправлен.")
                else:
                    print(f"Ошибка: {r.text}")
        except subprocess.CalledProcessError as e:
            print(f"ОШИБКА ClickHouse (код {e.returncode}):")
            print(e.stderr)
            sys.exit(1)


def main():
    # cmd = [
    #     'docker',
    #     'exec',
    #     '-u', '0',
    #     CONTAINER_NAME,
    #     'chown',
    #     '-R', 'clickhouse:clickhouse',
    #     '/var/lib/clickhouse'
    # ]
    # subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    check_container()
    tables = ['groups', 'sessions', 'users', 'raw']
    for table in tables:
        run_sql(table)

main()