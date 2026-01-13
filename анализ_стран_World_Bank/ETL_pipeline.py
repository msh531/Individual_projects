'''
Скрипт для выгрузки данных Всемирного банка (используется открытое API).

Функционал скрипта: 
- скачивает информацию обо всех странах
- скачивает информацию обо всех хранимых показателях Всемирного банка
- скачивает значения выбранных показателей для всех стран за выбранный период
- сохраняет информацию о странах и показателях в базу данных (Supabase)

Формат выходных данных:
таблицы в Supabase:
    - countries - данные о странах
    - indicators - перечень всех индикаторов с описанием
    - data - данные об значениях индикаторах стран за выбранный период

Настройка подключения:
параметры подключения к PostgreSQL/Supabase задаются через переменные окружения:
    - DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
'''
# импорт библиотек
import requests
import pandas as pd
import logging
from typing import List
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Создаем логгер
logger = logging.getLogger('my_logger')
logger.setLevel(logging.DEBUG)  

# Создаем обработчик для вывода в консоль (StreamHandler)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)  

# Создаем форматтер для красивого вывода сообщений
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Добавляем обработчик к логгеру
logger.addHandler(console_handler)

def fetch_data(parameter: str)-> pd.DataFrame | None:
    '''
    Функция получает данные о странах/индикаторах из API Всемирного банка.
    Параметры (str): 'country' - для выгрузки стран
                     'indicator' - для выгрузки индикаторов 
      
    Возвращает: pandas.DataFrame с данными или None в случае ошибки
    '''
    try: 
        base_url = 'https://api.worldbank.org/v2' # endpoint API 
        url = f"{base_url}/{parameter}"
        params = {'format': 'json',
                'per_page': 30000  # Большое значение для получения всех данных
           }
      
        # Выполняем запрос к API
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
    
        # Данные во втором элементе JSON - нормализуем его в датафрейм pandas 
        df_raw = pd.json_normalize(data[1])
    
        logger.info(f'Загружены данные в количестве: {df_raw.shape[0]}')
        return df_raw
    
    # Обрабатываем ошибки при обращении к API
    except requests.exceptions.RequestException as e:
        logger.error(f'Ошибка при запросе к API: {e}')
        return None
    except (KeyError, IndexError) as e:
        logger.error(f'Ошибка при обработке ответа API: {e}')
        return None
    
def countries_clearing(df_raw:pd.DataFrame)-> pd.DataFrame:
    '''
    Функция выполняет предобработку данных
    Параметры: pandas.DataFrame 
    Возвращает: обработанный pandas.DataFrame
    '''
   
    # оставляем  нужные для анализа столбцы и переименовываем их
    df = df_raw[['id', 'name', 'capitalCity',
                     'region.value', 'incomeLevel.value',
                     'lendingType.value']]
    
    df = df.rename(columns={'id':'iso3_code',
                            'capitalCity':'capital_city',
                            'region.value':'region',
                            'incomeLevel.value':'income_level',
                            'lendingType.value':'lending_type'})
    # удаляем дубликаты строк
    df = df.drop_duplicates()
    
    # удаляем аггрегированные регионы 
    df = df.loc[df['region']!='Aggregates']
    
    logger.info(f'Очищены данные о странах')    
    return df  

    
def indicators_clearing(df_raw:pd.DataFrame)-> pd.DataFrame:
    '''
    Функция выполняет предобработку данных
    Параметры: pandas.DataFrame 
    Возвращает: обработанный pandas.DataFrame
    '''
    
    # оставляем  нужные для анализа столбцы  и переименовываем их
    df = df_raw[['id', 'name', 'source.value','sourceNote']]
    df = df.rename(columns={'id':'indicator_id','source.value':'source','sourceNote':'source_note'})
    
    logger.info(f'Очищены данные об индикаторах')
    return df 

def make_countries_list(df: pd.DataFrame):
    ''' Формирует список кодов стран из датафрейма
        Параметры: pandas.DataFrame
        Возвращает список кодов стран'''
    countries_list = df['iso3_code'].tolist()
    logger.info(f'Создан список стран')
    return countries_list

def make_indicators_list():
    ''' Формирует список кодов индикаторов'''
    indicators_list = [
            'SP.POP.TOTL',
            'SP.POP.GROW',
            'SL.UEM.TOTL.NE.ZS',
            'SP.DYN.LE00.IN',
            'SI.POV.NAHC',
            'FP.CPI.TOTL.ZG',
            'NY.GDP.MKTP.CD',
            'NY.GDP.MKTP.KD.ZG',
            'NV.AGR.TOTL.ZS',
            'NV.IND.TOTL.ZS',
            'EG.USE.ELEC.KH.PC',
            'EG.ELC.ACCS.ZS',
            'EN.GHG.CO2.ZG.AR5',
            'EN.GHG.CO2.PC.CE.AR5',
            'IT.NET.USER.ZS',
            'SE.XPD.TOTL.GD.ZS',
            'GB.XPD.RSDV.GD.ZS',
            'BX.KLT.DINV.CD.WD',
            'NY.GDP.PCAP.CD',
            'NY.GDP.PCAP.KD.ZG',
            'NY.GDP.PCAP.PP.CD',
            'SH.ALC.PCAP.LI',
            'NY.GNP.ATLS.CD',
            'NY.GNP.PCAP.CD',
            'SI.POV.GINI',
            'SP.URB.TOTL.IN.ZS'
            ]
    logger.info(f'Создан список индикаторов')
    return indicators_list
    
def fetch_worldbank_data(indicators: List[str],
                         countries: List[str],
                         start_year: int,
                         end_year: int,
                         language: str = 'en') -> pd.DataFrame| None:

    '''
    Функция получает данные конкретных показателей по выбранным странам из API Всемирного банка
    за определенный период.

    Параметры:
        indicators: Список кодов показателей 
        countries: Список кодов стран 
        start_year: Год начала периода
        end_year: Год окончания периода
        language: en по умолчанию
    Возвращает:
        pandas.DataFrame с данными показателей или None в случае отсутствия 
    '''
    try:
        base_url = 'https://api.worldbank.org/v2' # endpoint API 

        # преобразовываем страны в строку с разделителем точка с запятой (для запроса данных)
        countries_str = ';'.join(countries)

        # список для хранения данных о показателях
        all_data = []
   
        for indicator in indicators:
            # Формируем URL для запроса
            url = f'{base_url}/{language}/country/{countries_str}/indicator/{indicator}'
            params = {
                'format': 'json',
                'date': f'{start_year}:{end_year}',
                'per_page': 10000  # Большое значение для получения всех данных
            }

            # Выполняем запрос к API
            response = requests.get(url, params=params)                           
            response.raise_for_status()
            data = response.json()

            # API возвращает массив, где первый элемент - метаданные, второй - данные
            if len(data) > 1 and isinstance(data[1], list):
                for item in data[1]:
                    all_data.append({
                            'country': item['country']['value'],
                            'iso3_code': item['countryiso3code'],
                            'indicator': item['indicator']['value'],
                            'indicator_code': item['indicator']['id'],
                            'year': int(item['date']),
                            'value': item['value']
                        })

        # Создаем DataFrame
        df = pd.DataFrame(all_data)

        logger.info(f'Выгружены значения индикаторов')
        return df

    # Обрабатываем возможные ошибки при работе с АПИ
    except requests.exceptions.RequestException as e:
        print(f'Ошибка при запросе к API: {e}')
        return None
    except (KeyError, IndexError, ValueError, TypeError) as e:
        print(f'Ошибка при обработке данных: {e}')
        return None

def load_to_base(df1: pd.DataFrame, 
                 df2: pd.DataFrame, 
                 df3: pd.DataFrame, 
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
    # Загрузка переменных окружения
    load_dotenv()

    # Параметры подключения
    USER = os.getenv('user')
    PASSWORD = os.getenv('password')
    HOST = os.getenv('host')
    PORT = '6543'
    DBNAME = os.getenv('dbname')

    # Адрес подключения
    DATABASE_URL = f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require'

    # Формируем движок
    engine = create_engine(DATABASE_URL) 

     
    try:
        with engine.connect() as connection:
            logger.info(f'Подключение успешно')
    except Exception as e:
        logger.error(f'Ошибка при подключении: {e}')

    try:
        df1.to_sql(df_name1, con=engine, if_exists='replace')
        df2.to_sql(df_name2, con=engine, if_exists='replace')
        df3.to_sql(df_name3, con=engine, if_exists='replace')
        logger.info(f'Загрузка данных успешна')
    except Exception as e:
        logger.error(f'Ошибка при загрузке: {e}')

if __name__ == '__main__': 
    
    countries_raw = fetch_data('country')  # Качаем все страны
    indicators_raw = fetch_data('indicator') # Качаем перечень всех индикаторов 
    countries = countries_clearing(countries_raw) # Предобработка данных о странах
    indicators = indicators_clearing(indicators_raw) #  Предобработка данных по индикаторам
    countries_list = make_countries_list(countries)  # Формирование списка стран
    indicators_list = make_indicators_list() # Формирование списка индикаторов

    # Загрузка значений выбранных индикаторов по спску стран за выбранный период
    data = fetch_worldbank_data(indicators = indicators_list,
                                countries = countries_list,
                                start_year = 1985,
                                end_year = 2024
                                )
    # Загрузка полученных данных в БД
    load_to_base(df1=countries, df2=indicators, df3=data, df_name1 = 'countries', df_name2='indicators', df_name3 = 'data') 

    logger.info(f'Скрипт успешно завершил работу') 