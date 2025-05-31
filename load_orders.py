import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import configparser
from glob import glob

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

engine = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')

# Создаем папку для датафреймов (если не существует)
os.makedirs(FILE_PATH, exist_ok=True)

files = glob(os.path.join(FILE_PATH, '*.csv'))

for file in files:
    df = pd.read_csv(
        file,
        encoding='UTF-16',
        parse_dates=['dateTime']
    )

    # преобразуем числовые данные
    for col in ['price_items', 'quantity_items', 'sum_items', 'totalSum']:
        df[col] = (
            df[col]
                .str.replace(',', '.')
                .astype(float)
        )

    # переименовываем class_NT в понятные термины
    norm_classNT = {
        'ФРОВ': 'фрукты, овощи',
        'МЛДП': 'молочная продукция и майонез',
        'БАКЛ': 'бакалея',
        'КНХЛ': 'кондитерские изделия',
        'НПТК': 'безалкогольные напитки',
        'МСНГ': 'колбасы',
        'ПРДЖ': 'зоотовары',
        'СЛАН': 'слабоалкогольные напитки',
        'МРЖН': 'мороженое',
        'ЧКФК': 'чай, кофе',
        'ПКЕТ': 'пакет',
        'NOFMCG': 'лекарства',
        'АЛКГ': 'алкоголь',
        'ТКОД': 'текстиль',
        'КЦИГ': 'игрушки и канцелярия',
        'ТАБК': 'табак',
        'КЗБХ': 'уход, гигиена и бытовая химия',
        'МЯСО': 'мясо и кулинария'
    }
    df['CLASS_NT'] = df['CLASS_NT'].replace(norm_classNT)

    # выделим в отдельные столбцы дату и день недели
    df['date'] = df['dateTime'].dt.date
    df['date'] = pd.to_datetime(df['date'])
    days = {0: 'Понедельник', 1: 'Вторник', 2: 'Среда',
            3: 'Четверг', 4: 'Пятница', 5: 'Суббота', 6: 'Воскресенье'}
    df['week_day'] = df['date'].dt.dayofweek.map(days)

    # исправляем неверную классификацию напитка (только в части категории первого уровня, т.к. при анализе мы пользовались только этим параметром)

    df['CLASS_NT'] = np.where(df['BASE_NT'] == 'АНАНАС ЗАМ', 'безалкогольные напитки', df['CLASS_NT'])

    # переименовываем колонку

    df = df.rename(columns={'Идентификатор покупателя': 'custom_id'})

print(df.info())

# Создаем таблицу, если уже существует, то перезаписываем
df.to_sql(
    'orders',
    engine,
    if_exists='append',
    index=False
)

# Закрываем соединение
engine.dispose()