import os
import glob
import psycopg2
from psycopg2 import sql
import configparser
import codecs
import csv
from io import StringIO

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
        # Создаем временную таблицу для загрузки
        temp_table = "temp_orders_" + os.path.basename(file).replace('.', '_')
        cur.execute(sql.SQL("""
                    CREATE TEMP TABLE {} AS 
                    SELECT * FROM orders LIMIT 0
                """).format(sql.Identifier(temp_table)))
        # Создаем буфер для преобразованных данных

        with open(file, 'r', encoding='utf-16') as f_orig:
            reader = csv.reader(f_orig)
            header = next(reader)  # Читаем заголовок
            numeric_columns = [6, 7, 8, 9 ]

            # Создаем буфер для порции данных
            buffer = StringIO()
            writer = csv.writer(buffer)
            row_count = 0
            batch_size = 5000  # Размер пакета для загрузки

            for row in reader:
                # Преобразуем числовые поля
                for idx in numeric_columns:
                    if idx < len(row):
                        # Заменяем запятую на точку и удаляем пробелы
                        row[idx] = row[idx].replace(',', '.').replace(' ', '')
                writer.writerow(row)
                row_count += 1

                # Загружаем порцию данных
                if row_count % batch_size == 0:
                    buffer.seek(0)
                    cur.copy_expert(
                        sql.SQL("COPY {} FROM STDIN WITH (FORMAT CSV, HEADER)").format(sql.Identifier(temp_table)),
                        buffer
                    )
                    buffer = StringIO()  # Сбрасываем буфер
                    writer = csv.writer(buffer)

            # Загружаем оставшиеся данные
            if buffer.tell() > 0:
                buffer.seek(0)
                cur.copy_expert(
                    sql.SQL("COPY {} FROM STDIN WITH (FORMAT CSV, HEADER)").format(sql.Identifier(temp_table)),
                    buffer
                )

            # Копируем данные из временной таблицы в основную
            cur.execute(sql.SQL("""
                            INSERT INTO orders 
                            SELECT * FROM {}
                        """).format(sql.Identifier(temp_table)))

            conn.commit()
            print(f"Successfully loaded {row_count} rows from {os.path.basename(file)}")
except Exception as e:
    print(f"Error: {e}")
    conn.rollback()

finally:
    if cur: cur.close()
    if conn: conn.close()