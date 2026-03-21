#!/usr/bin/env python
# coding: utf-8

# In[1]:


''' 
Скрипт выгрузки данных по городам, их предарительной очистки, дополнения координатами и загрузки в БД. 
Скрипт скачивает данные по городам РФ из Wikipedia (включая регион и численность населения) - формирует pandas.DataFrame,
проводит предварительную очистку данных - формирует очищенный pandas.DataFrame,
добавляет координаты городов из Яндекс геокодера - формирует pandas.DataFrame.
Все три датафрейма загружает в БД (Supabase)
''' 

import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


# Настройка логов
def setup_logging() -> logging.Logger:

    '''Настройка логирования.'''

    log_filename = 'city_script.log'
    logger = logging.getLogger('city_collector')
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

# Выгрузка списка городов России из Wikipedia
def fetch_cities() -> pd.DataFrame:

    '''
    Извлекает список городов России с указанием региона и численности населения со страницы Википедии.

   Выполняет следующие действия:
    1. Отправляет GET‑запрос к API Википедии для получения HTML‑разметки страницы
       «Список городов России».
    2. Парсит HTML, находит сортируемую таблицу с данными.
    3. Извлекает из каждой строки таблицы:
       - название города,
       - регион,
       - численность населения.
    4. Формирует и возвращает pandas.DataFrame с тремя колонками:
       'city', 'region', 'population'.
    '''

    url = 'https://ru.wikipedia.org/w/api.php'  # базовый URL
    params = {
        'action': 'parse',
        'page': 'Список_городов_России',
        'format': 'json'
    }
    headers = {'User-Agent': 'PythonScript/1.0'} # указываем User-Agent, это требование Wikipedia    

    try:
        logger.info(f'Извлекаем данные из HTML-кода страницы: {url}')
        response = requests.get(url, params = params, headers=headers, timeout=10)

        # Проверяем статус
        if response.status_code == 200:
            data = response.json()
            logger.info('Данные успешно получены от API Википедии')
        else:
            logger.error(f'Ошибка загрузки: {response.status_code}:{response.text}')

    except requests.exceptions.RequestException as e:
        logger.error(f'Ошибка при получении данных от API Википедии: {e}')


 # парсим полученные данные
    try:
        logger.info('Парсинг HTML-кода для извлечения таблицы.')
        html_text = data['parse']['text']['*']
        soup = BeautifulSoup(html_text, 'html.parser')
        table = soup.find('table', {'class': 'sortable'})
        # Если таблиц нет - ошибка
        if not table:
            logger.error('Таблица не найдена на странице.')


        rows = table.find_all('tr')[1:] # пропускаем заголовок
        cities = []
        regions = []
        population = []

        for row in rows:
            cells = row.find_all('td') 
            if cells:
                city_cell = cells[2] # название города
                city_name = city_cell.get_text(strip=True)

                region_cell = cells[3] # название региона
                region_name = region_cell.get_text(strip=True) 

                population_cell = cells[5] # численность населения
                population_name = population_cell.get_text(strip=True)

            cities.append(city_name)
            regions.append(region_name)
            population.append(population_name)

        # создаем DataFrame
        df_cities = pd.DataFrame({
        'city': cities,
        'region': regions,
        'population': population
            })
        logger.info(f'Датафрейм с городами России создан, строк:{len(df_cities)}')
        return df_cities

    except Exception as e:
        logger.error(f'Ошибка данных: {e}')


# Предобработка данных городов
def clean_cities(df_cities: pd.DataFrame) -> pd.DataFrame:

    ''' 
    Функция чистит датафрейм с городами: приводит в порядок названия, типы данных
    Убирает мелкие города, деревни с населеним менее 30 тыс. жителей
    Добавляет пустые столбцы для дальнейшей загрузки координат
    Dозвращает очищенный pandas.DataFrame с пятью колонками:
       'city', 'region', 'population', 'lat', 'lon'
    '''

    try:
        logger.info('Чистим данные по городам')
        df_cities['city'] = df_cities['city'].str.replace('не призн.', '', regex=False).str.strip()
        df_cities['region'] = df_cities['region'].str.strip()
        df_cities['population'] = (
            df_cities['population']
            .astype(str)
            .str.replace(r'[^\d\s]|', '', regex=True)
            .str.replace(r'\s+', '', regex=True)
            .str.strip()
        )
        df_cities['population'] = pd.to_numeric(df_cities['population'], errors='coerce')
        df_cities = df_cities.loc[df_cities['population']>=30000].copy()

        # добавляем пустые колонки для коородинат
        df_cities['lat'], df_cities['lon'] = None, None
        logger.info('Предобработка городов успешна')
        return df_cities

    except Exception as e:
        logger.error(f'Ошибка обработки городов {e}')  



# Получение координат для городов 
def add_coordinates_to_cities(
    df: pd.DataFrame,
    apikey: str,
    city_column: str = 'city',
    region_column: str = 'region',
    delay: float = 0.1
) -> pd.DataFrame:

    '''
    Добавляет координаты (долготу и широту) к датафрейму на основе названий городов и регионов.
    Учитывает дубликаты городов в разных регионах.

    Параметры:
    df: pd.DataFrame — входной датафрейм с колонками 'город', 'регион', 'население', 'lat', 'lon'.
    apikey: str — API‑ключ для доступа к геокодеру Яндекса.
    city_column: str — название столбца с городами (по умолчанию 'city').
    region_column: str — название столбца с регионами (по умолчанию 'region').
    delay: float — задержка между запросами в секундах (для соблюдения лимитов API).

    Возвращает:
    pd.DataFrame — датафрейм с заполненными колонками 'lon' и 'lat'.
  '''

    # Создаём копию датафрейма
    result_df = df.copy()

    base_url = 'https://geocode-maps.yandex.ru/1.x'

    logger.info(f'Начало обработки {len(result_df)} записей')

    # Словарь для кэширования результатов по комбинации город+регион
    cache = {}

    for idx, row in tqdm(result_df.iterrows(), total=len(result_df), desc='Обработка записей'):
        city_name = str(row[city_column]).strip()
        region_name = str(row[region_column]).strip()

        # Пропускаем пустые значения
        if not city_name or city_name.lower() in ['nan', 'none', '']:
            logger.debug(f'Пропущен пустой город в строке {idx}')
            continue
        if not region_name or region_name.lower() in ['nan', 'none', '']:
            region_name = ''

        # Формируем уникальный ключ для кэша
        cache_key = f"{city_name}_{region_name}"

        # Проверяем кэш
        if cache_key in cache:
            lon, lat = cache[cache_key]
            result_df.at[idx, 'lon'] = lon
            result_df.at[idx, 'lat'] = lat
            logger.debug(f'Использованы кэшированные координаты для {city_name}, {region_name}: {lon}, {lat}')
            continue

        try:
            # Формируем запрос с учётом региона для точности
            address = f"{city_name}, {region_name}" if region_name else city_name

            response = requests.get(
                base_url,
                params={
                    'geocode': address,
                    'apikey': apikey,
                    'format': 'json',
                },
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            # Извлекаем данные
            found_places = data['response']['GeoObjectCollection']['featureMember']

            if not found_places:
                logger.warning(f'Адрес не найден: {address}')
                cache[cache_key] = (None, None)
                continue

            most_relevant = found_places[0]

            # Парсим координаты
            try:
                pos = most_relevant['GeoObject']['Point']['pos']
                lon, lat = map(float, pos.split(' '))
                result_df.at[idx, 'lon'] = lon
                result_df.at[idx, 'lat'] = lat
                # Сохраняем в кэш
                cache[cache_key] = (lon, lat)
                logger.debug(f'Координаты для {address}: {lon}, {lat}')
            except (KeyError, ValueError) as e:
                logger.warning(f'Ошибка парсинга координат для {address}: {e}')
                cache[cache_key] = (None, None)
                continue

        except requests.exceptions.RequestException as e:
            logger.error(f'Ошибка запроса для {address}: {e}')
            cache[cache_key] = (None, None)
            continue
        except Exception as e:
            logger.error(f'Неожиданная ошибка для {address}: {e}')
            cache[cache_key] = (None, None)
            continue

        # Задержка между запросами
        time.sleep(delay)

    # Подсчёт результатов
    successful_coords = result_df['lon'].notna().sum()
    total_records = len(result_df)

    logger.info(
        f'Обработка завершена. Координаты добавлены для {successful_coords} из {total_records} записей. '
        f'Успешность: {successful_coords / total_records * 100:.1f}%'
    )
    return result_df

# Подключение к БД Supabase
def load_df_to_DB (df_1: pd.DataFrame, 
                   df_2: pd.DataFrame, 
                   df_3: pd.DataFrame, 
                    df_name1: str,
                    df_name2: str,
                    df_name3: str,
                    ) -> None:
    '''
    Функция загружает DataFrames в базу данных.

    Параметры:
    df1,2,3 (DataFrame): датафреймы  для сохранения в базу данных
    df_name1,2,3 (str): название таблиц для сохранения данных в БД

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
        logger.info(f'Загружаем все данные в базу')

        df_1.to_sql('df_name1', con=engine, if_exists='replace')
        logger.info(f'Загрузка городов успешна')

        df_2.to_sql('df_name2', con=engine, if_exists='replace')
        logger.info(f'Загрузка очищенных городов успешна')

        df_3.to_sql('df_name3', con=engine, if_exists='replace')
        logger.info(f'Загрузка городов с координатами успешна ')
    except Exception as e:
        logger.error(f'Ошибка при загрузке: {e}')


# In[2]:


# Основной блок 
if __name__ == '__main__':
    try:
        df_cities = fetch_cities()
        logger.info(f'Получено {len(df_cities)} городов из Википедии')

        clean_cities = clean_cities(df_cities)
        logger.info(f'После очистки осталось {len(clean_cities)} городов')

        result_df = add_coordinates_to_cities(clean_cities,'ec22de73-62ee-4a24-affb-ad4eea3226b4')
        logger.info(f'Координаты добавлены для {result_df["lon"].notna().sum()} городов')

        load_df_to_DB(df_1=df_cities, df_2=clean_cities, df_3=result_df, df_name1 = 'wiki_cities', df_name2='clean_cities', df_name3 = 'cities_coords') 
        logger.info('Все данные успешно загружены в базу')

        logger.info('Скрипт успешно выполнен.')
    except Exception as e:
        logger.error(f'Ошибка при выполнении скрипта: {e}')
        raise


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




