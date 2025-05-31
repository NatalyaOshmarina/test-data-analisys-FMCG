import os
import glob
import psycopg2
from psycopg2 import sql
import configparser
import codecs
import csv
import io

# прописываем абсолютный путь до исполняемого файла
dirname = os.path.dirname(__file__)
config = configparser.ConfigParser()
config.read(os.path.join(dirname, 'config.ini'))
path = config['Paths']['ORDERS_PATH']
FILE_PATH = os.path.join(dirname, path)

# Создаем подключение к базе Postgres
HOST = config['Access']['HOST_SQL']
PORT = 5432
DATABASE = config['Access']['BASE_SQL']
USER = config['Access']['USER_SQL']
PASSWORD = config['Access']['PASSWORD_SQL']

# Создаем папку для датафреймов (если не существует)
os.makedirs(FILE_PATH, exist_ok=True)

files = glob.glob(os.path.join(FILE_PATH, '*.csv'))

try:
    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        dbname=DATABASE,
        user=USER,
        password=PASSWORD
    )
    cur = conn.cursor()

    for file in files:
        # Создаем буфер для преобразованных данных
        output = io.StringIO()
        writer = csv.writer(output)
        with open(file, 'r', encoding='utf-16') as f_orig:
            reader = csv.reader(f_orig)
            header = next(reader)  # Читаем заголовок
            numeric_columns = [6, 7, 8, 9 ]

            writer.writerow(header)  # Пишем заголовок без изменений
            for row_idx, row in enumerate(reader, start=2):
                # Преобразуем числовые поля
                for idx in numeric_columns:
                    if idx < len(row):
                        # Заменяем запятую на точку и удаляем пробелы
                        row[idx] = row[idx].replace(',', '.').replace(' ', '')
                writer.writerow(row)

        # Сбрасываем буфер в начало
        output.seek(0)
        cur.copy_expert(
            sql.SQL("COPY orders FROM STDIN WITH (FORMAT CSV, HEADER)"),
            output
        )
        conn.commit()
        print(f"Successfully loaded {os.path.basename(file)}")

except Exception as e:
    print(f"Error: {e}")
    conn.rollback()

finally:
    if cur: cur.close()
    if conn: conn.close()