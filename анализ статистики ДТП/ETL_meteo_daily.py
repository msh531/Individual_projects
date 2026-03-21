#!/usr/bin/env python
# coding: utf-8

# In[4]:


''' 
Скрипт выгрузки данных о погоде за перид 2015-2025 вкдючительно по выбранным городам. 
Скрипт скачивает данные по предварительно выбранным городам РФ из API Open-meteo - формирует json,
преобразует json в pandas.DataFrame,
загружает pandas.DataFrame в БД (Supabase)
''' 


# импорт библиотек
import logging
import requests
import time
from datetime import datetime, timedelta
import pandas as pd
from typing import List, Dict, Optional
from pandas import json_normalize
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Настройка логов
def setup_logging() -> logging.Logger:

    '''Настройка логирования.'''

    log_filename = 'meteo_script.log'
    logger = logging.getLogger('meteo_collector')
    logger.setLevel(logging.INFO)

    # Очистка существующих обработчиков
    if logger.handlers:
        logger.handlers.clear()

    # Обработчик для файла
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Обработчик для консоли
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    return logger

logger = setup_logging()

# Подключение к БД
def connection_to_DB():
    load_dotenv()

    # Параметры подключения
    user = os.getenv('user')
    password = os.getenv('password')
    host = os.getenv('host')
    port = '6543'
    dbname = os.getenv('dbname')

    # Адрес подключения
    DATABASE_URL = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}?sslmode=require"

    # Формируем движок
    engine = create_engine(DATABASE_URL) 

    try:
        with engine.connect() as connection:
            logger.info('Подключение к базе успешно')
        return engine  
    except Exception as e:
        logger.error(f'Ошибка при подключении к базе: {e}')
        return None
# Получаем движок
engine = connection_to_DB()

# запрос городов
query = '''
    SELECT city, lat, lon
    FROM cities_coords
   where city in ('Екатеринбург', 'Владивосток');
'''
cities = pd.read_sql_query(query, con=engine)

# Выгрузка данных о погоде
def get_open_meteo_data(df:pd.DataFrame):
    '''
    Функция получает данные через API Open-meteo за период 2015-2025 включительно
    по выбранным городам. Даннные о погоде по дням.
    '''
    all_cities_data = {}

    for idx, row in df.iterrows():
        city_name = row['city']
        latitude = row['lat']
        longitude = row['lon']

        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
    	"latitude": latitude,
    	"longitude": longitude,
    	"start_date": "2015-01-01",
    	"end_date": "2026-01-01",
    	"daily": ["temperature_2m_mean", "temperature_2m_min", 
              "wind_speed_10m_max", "precipitation_sum", "rain_sum", 
              "snowfall_sum", "precipitation_hours", "temperature_2m_max"],
    	"timezone": "auto",
        }
        try:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()

                all_cities_data[city_name] = data

                logger.info(f'Успешно загружены данные для города')
            else:
                logger.error(f'Ошибка API:{response.status_code}')
        except Exception as e:
            logger.exception(f'Oшибка при обработке городов: {e}')

    return all_cities_data

# Парсинг данных о погоде
def parsing_meteo_data (data) -> pd.DataFrame:
    '''
    Преобразует json с метеоданными (из API) в единый DataFrame.
    Возвращает:
        pd.DataFrame с колонками:
            - city (название города)
            - latitude, longitude (координаты)
            - date (дата замера)
            - все метеопараметры (temperature_2m, snow_depth и т.д.)
    '''
    df = pd.json_normalize(data)
    df.columns = df.columns.str.rsplit('.', n=1).str[-1]
    df1 = df.iloc[:,[0,1,16,17,18,19,20,21,22,23,24]]
    df2 = df.iloc[:,[25,26,41,42,43,44,45,46,47,48,49]]
    col_numbers = [2,3,4,5,6,7,8,9,10]
    col_names1 = df1.columns[col_numbers].tolist()
    col_names2 = df2.columns[col_numbers].tolist()
    df1 = df1.explode(col_names1)
    df2 = df2.explode(col_names2)
    df1['city'] = 'Владивосток'
    df2['city'] = 'Екатеринбург'
    return df1, df2


# Подключение к БД Supabase
def load_df_to_DB (df: pd.DataFrame,

                   df_name: str,
                   ) :
    '''
    Функция загружает DataFrame в базу данных.

    Параметры:
    df (DataFrame): датафрейм  для сохранения в базу данных
    df_name (str): название таблиц для сохранения данных в БД

    ''' 

    load_dotenv()

    # Параметры подключения
    user = os.getenv('user')
    password = os.getenv('password')
    host = os.getenv('host')
    port = '6543'
    dbname = os.getenv('dbname')

    # Адрес подключения
    DATABASE_URL = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}?sslmode=require"

    # Формируем движок
    engine = create_engine(DATABASE_URL) 

    try:
        with engine.connect() as connection:
            logger.info('Подключение к базе успешно')
    except Exception as e:
            logger.error(f'Ошибка при подключении к базе: {e}')

    # загрузка данных в базу
    try:

        df1.to_sql('Vladivostok', con=engine, if_exists='replace') 
        df1.to_sql('Ekaterinburg', con=engine, if_exists='replace')  
        logger.info(f'Загружены все данные в базу')    
    except Exception as e:
        logger.error(f'Ошибка при загрузке: {e}')


# Основной блок 
if __name__ == '__main__':
    try:
        connection_to_DB()
        logger.info(f'Подключение к базе успешно')

        query = '''
        SELECT city, lat, lon
        FROM cities_coords
        where city in ('Екатеринбург', 'Владивосток');
        '''
        cities = pd.read_sql_query(query, con=engine)
        logger.info(f'Загрузка из базы завершена.')     

        data = get_open_meteo_data(cities)
        logger.info(f'Загрузка погоды завершена.')

        df = parsing_meteo_data(data)
        df = parsing_meteo_data(data)
        logger.info(f'Обработка погоды в датафрейм. Успешно.')

        load_df_to_DB(df=df, df_name = 'meteo_data_1') 
        logger.info('Все данные о погоде успешно загружены в базу')

        logger.info('Скрипт успешно выполнен.')
    except Exception as e:
        logger.error(f'Ошибка при выполнении скрипта: {e}')
        raise


# In[ ]:





# In[ ]:




