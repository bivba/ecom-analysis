import subprocess
import os
import sys

CONTAINER_NAME = 'clickhouse-server'
SQL_DIR = './sql'

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

    

def run_sql_file(file_path):
    if not os.path.exists(file_path):
        print(f"файл {file_path} не найден.")
        return
    print(f'Выполнение файла {file_path}')

    cmd = ["docker", "exec", "-i", CONTAINER_NAME, "clickhouse-client", "-n"]

    try:
        with open(file_path, 'r') as file:
            subprocess.run(
                cmd,
                stdin=file,
                check=True,
                text=True,
                capture_output=True
            )
            print(f"Успешно выполнен {file_path}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to execute {file_path}: {e}")
        sys.exit(1)

def main():
    check_container()

    for sql_file in sorted(os.listdir(SQL_DIR)):
        if sql_file.endswith('.sql'):
            if sql_file == 'insert_values.sql':
                continue
            run_sql_file(os.path.join(SQL_DIR, sql_file))
    
    print('Успешная нициализация базы данных')

if __name__ == "__main__":
    main()