#!/usr/bin/env python
# coding: utf-8

# In[8]:


'''
Скрипт выгрузки данных о погоде за период 2015-2025 включительно по выбранным городам. 
Скрипт скачивает данные по предварительно выбранным городам РФ из API Open-meteo - формирует json,
преобразует json в pandas.DataFrame,
загружает pandas.DataFrame в БД (Supabase) или в CSV файлы при недоступности БД
''' 

# импорт библиотек
import logging
import requests
import time
from datetime import datetime, timedelta
import pandas as pd 
from typing import List, Dict, Optional, Tuple
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

# Функция загрузки городов из CSV
def load_cities_from_csv(csv_path: str = 'cities_coords_rows.csv') -> pd.DataFrame:
    '''
    Загружает данные о городах из CSV файла.
    Ожидаемые колонки: city, lat, lon
    '''
    try:
        cities_df = pd.read_csv(csv_path)
        # Фильтруем нужные города
        cities_df = cities_df[cities_df['city'].isin(['Екатеринбург', 'Владивосток'])]
        logger.info(f'Данные о городах успешно загружены из CSV файла {csv_path}')
        return cities_df
    except Exception as e:
        logger.error(f'Ошибка при загрузке городов из CSV: {e}')
        raise

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

# Функция загрузки городов (с приоритетом БД, при недоступности - CSV)
def load_cities(engine) -> pd.DataFrame:
    '''
    Загружает данные о городах.
    Сначала пытается загрузить из БД, при недоступности использует CSV файл.
    '''
    if engine is not None:
        try:
            query = '''
                SELECT city, lat, lon
                FROM cities_coords
                where city in ('Екатеринбург', 'Владивосток');
            '''
            cities = pd.read_sql_query(query, con=engine)
            logger.info('Данные о городах загружены из базы данных')
            return cities
        except Exception as e:
            logger.warning(f'Ошибка при загрузке городов из БД: {e}. Пытаемся загрузить из CSV...')
            return load_cities_from_csv()
    else:
        logger.info('Подключение к БД недоступно. Загружаем города из CSV файла...')
        return load_cities_from_csv()

# Выгрузка данных о погоде
def get_open_meteo_data(df: pd.DataFrame) -> Dict:
    '''
    Функция получает данные через API Open-meteo за период 2015-2025 включительно
    по выбранным городам. Данные о погоде по дням.
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
                logger.info(f'Успешно загружены данные для города {city_name}')
                time.sleep(1)  # Пауза между запросами
            else:
                logger.error(f'Ошибка API для города {city_name}: {response.status_code}')
        except Exception as e:
            logger.exception(f'Ошибка при обработке города {city_name}: {e}')

    return all_cities_data

# Парсинг данных о погоде - возвращает словарь с двумя DataFrame
def parsing_meteo_data(data: Dict) -> Dict[str, pd.DataFrame]:
    '''
    Преобразует json с метеоданными (из API) в словарь DataFrame для каждого города.
    Возвращает:
        Dict[str, pd.DataFrame] - словарь, где ключ - название города, значение - DataFrame с погодой
    '''
    cities_dfs = {}

    for city_name, city_data in data.items():
        try:
            # Проверяем наличие данных daily
            if 'daily' not in city_data:
                logger.error(f'Нет ключа daily в данных для города {city_name}')
                logger.info(f'Доступные ключи: {list(city_data.keys())}')
                continue

            daily_data = city_data['daily']

            # Создаем DataFrame из daily данных
            daily_df = pd.DataFrame(daily_data)

            # Переименовываем колонки для удобства
            column_mapping = {
                'time': 'date',
                'temperature_2m_mean': 'temperature_mean',
                'temperature_2m_min': 'temperature_min',
                'temperature_2m_max': 'temperature_max',
                'wind_speed_10m_max': 'wind_speed_max',
                'precipitation_sum': 'precipitation_sum',
                'rain_sum': 'rain_sum',
                'snowfall_sum': 'snowfall_sum',
                'precipitation_hours': 'precipitation_hours'
            }

            # Переименовываем только существующие колонки
            existing_mapping = {old: new for old, new in column_mapping.items() 
                               if old in daily_df.columns}
            daily_df = daily_df.rename(columns=existing_mapping)

            # Добавляем информацию о городе и координатах
            daily_df['city'] = city_name
            daily_df['latitude'] = city_data.get('latitude')
            daily_df['longitude'] = city_data.get('longitude')

            # Переупорядочиваем колонки
            columns_order = ['city', 'latitude', 'longitude', 'date', 
                           'temperature_mean', 'temperature_min', 'temperature_max',
                           'wind_speed_max', 'precipitation_sum', 'rain_sum', 
                           'snowfall_sum', 'precipitation_hours']

            # Оставляем только существующие колонки
            existing_columns = [col for col in columns_order if col in daily_df.columns]
            daily_df = daily_df[existing_columns]

            cities_dfs[city_name] = daily_df
            logger.info(f'Успешно обработан город {city_name}. Получено {len(daily_df)} записей')

        except Exception as e:
            logger.error(f'Ошибка при обработке города {city_name}: {e}')
            logger.exception('Детали ошибки:')
            continue

    if not cities_dfs:
        logger.error('Не удалось получить данные ни для одного города')

    return cities_dfs

# Сохранение DataFrame в CSV файл
def save_to_csv(df: pd.DataFrame, filename: str):
    '''
    Сохраняет DataFrame в CSV файл.
    '''
    try:
        df.to_csv(filename, index=False, encoding='utf-8')
        logger.info(f'Данные успешно сохранены в файл {filename}')
        logger.info(f'Сохранено {len(df)} записей для города {df["city"].iloc[0] if not df.empty else "Unknown"}')
    except Exception as e:
        logger.error(f'Ошибка при сохранении в CSV {filename}: {e}')
        raise

# Загрузка данных в БД или CSV
def load_data_to_db_or_csv(cities_dfs: Dict[str, pd.DataFrame], engine):
    '''
    Загружает данные в БД (если доступна) или в CSV файлы.

    Параметры:
    cities_dfs (Dict[str, pd.DataFrame]): словарь с DataFrame для каждого города
    engine: подключение к БД
    '''
    if not cities_dfs:
        logger.error('Нет данных для загрузки')
        return False

    if engine is not None:
        try:
            with engine.connect() as connection:
                # Загружаем каждый DataFrame в отдельную таблицу в БД
                for city_name, df in cities_dfs.items():
                    table_name = f"{city_name.lower()}_weather"
                    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
                    logger.info(f'Данные для города {city_name} загружены в таблицу {table_name}')
                logger.info('Все данные успешно загружены в базу данных')
                return True
        except Exception as e:
            logger.error(f'Ошибка при загрузке в БД: {e}')
            logger.info('Сохраняем данные в CSV файлы...')
            save_to_csv_files(cities_dfs)
            return False
    else:
        logger.info('Подключение к БД недоступно. Сохраняем данные в CSV файлы...')
        save_to_csv_files(cities_dfs)
        return False

def save_to_csv_files(cities_dfs: Dict[str, pd.DataFrame]):
    '''
    Сохраняет каждый DataFrame в отдельный CSV файл.
    '''
    for city_name, df in cities_dfs.items():
        filename = f"{city_name.lower()}_weather.csv"
        save_to_csv(df, filename)

# Основной блок 
if __name__ == '__main__':
    try:
        # Подключаемся к БД (если возможно)
        engine = connection_to_DB()

        # Загружаем города (из БД или CSV)
        cities = load_cities(engine)
        logger.info(f'Загружены города: {cities["city"].tolist()}')

        # Получаем данные о погоде
        weather_data = get_open_meteo_data(cities)
        logger.info('Загрузка погоды завершена')

        # Парсим данные - получаем словарь с DataFrame для каждого города
        cities_weather_dfs = parsing_meteo_data(weather_data)

        if cities_weather_dfs:
            # Выводим информацию о полученных данных
            for city_name, df in cities_weather_dfs.items():
                logger.info(f'Город {city_name}: {df.shape[0]} записей, {df.shape[1]} колонок')


            # Загружаем данные (в БД или CSV)
            load_data_to_db_or_csv(cities_weather_dfs, engine)

            logger.info('Скрипт успешно выполнен')
        else:
            logger.error('Не удалось получить данные о погоде ни для одного города')

    except Exception as e:
        logger.error(f'Ошибка при выполнении скрипта: {e}')
        raise


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




