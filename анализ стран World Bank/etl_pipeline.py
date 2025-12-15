
# импорт бтблиотек
import requests
import pandas as pd
import numpy as np
from typing import List, Optional
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

def fetch_data(parametr):
    '''
    Функция получает данные о странах/индикаторах из API Всемирного банка.
    Параметры (str): 'country' - для выгрузки стран
                     'indicator' - для выгрузки индикаторов 
      
    Возвращает: pandas.DataFrame с данными 
    '''

    base_url = "https://api.worldbank.org/v2" # endpoint API 
    url = f"{base_url}/{parametr}"
    params = { 'format': 'json',
                'per_page': 30000  # Большое значение для получения всех данных
           }
      
    # Выполняем запрос к API
    response = requests.get(url, params=params)
    
    # обработка ошибок
    if response.status_code == 200:
        data = response.json()
    else:
        print(f"Ошибка при запросе: {response.status_code}")
        print(f"Текст ошибки: {response.text}")   

    # формируем датафрейм из полученных данных 
    df_raw = pd.DataFrame(data[1])
    
    # проверка на пустоту
    if df_raw.empty:
        print("Нет данных")
        return None  
    
    return df_raw   


def countries_clearing(df_raw):
    '''
    Выполняет предобработку данных
    Параметры: pandas.DataFrame 
    Возвращает: обработанный pandas.DataFrame
    '''
    # выделяем идентификатор региона из столбца 
    df_raw['region_id'] = df_raw.region.apply(lambda x: x.get('id', None))
    
    # аналогично выделяем информацию из других столбцов
    for col in ['region', 'adminregion', 'incomeLevel', 'lendingType']:
        df_raw[col + '_value'] = df_raw[col].apply(lambda cell: cell.get('value', None))

    # оставляем  нужные для анализа столбцы и переименовываем их
    df = df_raw[['id', 'iso2Code', 'name', 'capitalCity',
                     'region_id', 'region_value', 'incomeLevel_value',
                     'lendingType_value']]
    
    df = df.rename(columns={'id':'id_cnt',
                            'iso2Code':'iso2_code',
                            'capitalCity':'capital_city',
                            'incomeLevel_value':'income_level_value',
                            'lendingType_value':'lending_type_value'})
    # удаляем дубликаты строк
    df = df.drop_duplicates()
    
    # удаляем аггрегированные регионы 
    df = df.loc[df['region_value']!='Aggregates']
  
    return df

    
def indicators_clearing(df_raw):
    '''
    Выполняет предобработку данных
    Параметры: pandas.DataFrame 
    Возвращает: обработанный pandas.DataFrame
    '''
    # выделяем значение источника из столбца 
    df_raw['source'] = df_raw.source.apply(lambda x: x.get('value', None))

    # оставляем  нужные для анализа столбцы 
    df = df_raw[['id', 'name', 'source','sourceNote']]

    df = df.rename(columns={'id':'id_ind','sourceNote':'source_note'})
    
    return df 

def make_countries_list(df):
    ''' Формирует список кодов стран '''
    countries_list = df['id_cnt'].tolist()
    return countries_list

def make_indicators_list():
    ''' Формирует список кодов индикаторов '''
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
    return indicators_list
    
def fetch_worldbank_data(indicators: List[str],
                         countries: List[str],
                         start_year: int,
                         end_year: int,
                         language: str = 'en') -> Optional[pd.DataFrame]:

    '''
    Функция получает данные показателей из API Всемирного банка.

    Параметры:
        indicators: Список кодов показателей 
        countries: Список кодов стран в формате ISO 2
        start_year: Год начала периода
        end_year: Год окончания периода
        language: Язык данных 

    Возвращает:
        pandas.DataFrame с данными показателей
    '''

    base_url = "https://api.worldbank.org/v2" # endpoint API 

    # преобразовываем страны в строку с разделителем точка с запятой (для запроса данных)
    countries_str = ';'.join(countries)

    # список для хранения данных о показателях
    all_data = []

    try:
        for indicator in indicators:
            # Формируем URL для запроса
            url = f"{base_url}/{language}/country/{countries_str}/indicator/{indicator}"
            params = {
                'format': 'json',
                'date': f"{start_year}:{end_year}",
                'per_page': 10000  # Большое значение для получения всех данных
            }

            # Выполняем запрос к API
            response = requests.get(url, params=params)
            
            if response.status_code == 404:
                    print(f"Данные не найдены для индикатора {indicator}")
                    break
            
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

        if df.empty:
            print("Предупреждение: Не получено данных для указанных параметров")
            return None

        return df

    # Обрабатываем возможные ошибки при работе с АПИ
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при запросе к API: {e}")
        return None
    except (KeyError, IndexError, ValueError, TypeError) as e:
        print(f"Ошибка при обработке данных: {e}")
        return None

def load_to_supabase(countries, indicators, data):
    """
    Загружает DataFrames в Supabase через SQLAlchemy.
    
    Параметры:
    countries, indicators, data — pandas.DataFrame для загрузки
    """
    # Load environment variables from .env
    load_dotenv()

    # Fetch variables
    USER = os.getenv("user", 'postgres.rfhnmbttgskiamdmzjyh')
    PASSWORD = os.getenv("password",'pst484971')
    HOST = os.getenv("host",'aws-1-eu-central-1.pooler.supabase.com')
    PORT = "6543"
    DBNAME = os.getenv("dbname",'postgres')

    # Construct the SQLAlchemy connection string
    DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

    # Create the SQLAlchemy engine
    engine = create_engine(DATABASE_URL) 

    # Test the connection
    try:
        with engine.connect() as connection:
            print("Connection successful!")
    except Exception as e:
        print(f"Failed to connect: {e}")

    try:
        countries.to_sql('countries', con=engine, if_exists='replace')
        indicators.to_sql('indicators', con=engine, if_exists='replace')
        data.to_sql('data', con=engine, if_exists='replace')
        print('Load successfull')
    except Exception as e:
        print(f"Неожиданная ошибка при загрузке: {e}")
        return False  
  
def run_etl():
    """
    ETL-pipeline: Extract → Transform → Load
    """
    # выгрузка 
    try:
        countries_raw = fetch_data('country')
        indicators_raw = fetch_data('indicator')
        print('Raw data loaded')    
    except Exception as e:
        print(f'Raw load mistake: {e}')
        return
    
    # предобработка
    try:
        countries = countries_clearing(countries_raw)
        indicators = indicators_clearing(indicators_raw)
        print('Clearing data done')
    except Exception as e:
        print(f'Clearing data mistake: {e}')
        return
    
    # формирование списков
    try:
        countries_list = make_countries_list(countries)
        indicators_list = make_indicators_list()
        print('Lists done')
    except Exception as e:
        print(f'Lists mistake: {e}')
        return
    
    # загрузка значений индикаторов по спскам
    try:
        data = fetch_worldbank_data(indicators = indicators_list,
                                countries = countries_list,
                                start_year = '1985',
                                end_year = '2024',
                                )
        print('WB data done')
    except Exception as e:
        print(f'WB data mistake: {e}')
        return
    
    # загрузка полученных данных в БД
    try:
        load_to_supabase(countries, indicators, data)
        print('Load done')
    except Exception as e:
        print(f'Load mistake: {e}')

       
