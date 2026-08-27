#!/usr/bin/env python
# coding: utf-8

# In[1]:


''' Скрипт загружает данные из csv файла в облачную базу данных'''

import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Создаем логгер
logging.basicConfig( 
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s" 
    ) 
logger = logging.getLogger(__name__)

def load_csv_to_table(csv_path, table_name, delimiter=',', encoding='utf-8', if_exists='replace'):
    '''
    Загружает данные из CSV-файла в указанную таблицу в БД.

    Параметры:
        csv_path (str): путь к CSV-файлу
        table_name (str): имя таблицы в БД
        delimiter (str): разделитель полей (по умолчанию ',')
        encoding (str): кодировка файла (по умолчанию 'utf-8')
        if_exists (str): 'replace' — пересоздать таблицу, если уже существует
    '''
    # Загружаем переменные из файла .env
    load_dotenv()  
    # подключение к БД
    DATABASE_URL = os.getenv("DATABASE_URL")
    engine = create_engine(DATABASE_URL)

    try:
        with engine.connect() as connection:
            logger.info(f'Подключение к БД успешно')
    except Exception as e:
        logger.error(f'Ошибка при подключении к БД: {e}')


    # Читаем CSV 
    try: 
        df = pd.read_csv(csv_path, delimiter=delimiter, encoding=encoding ) 
        logger.info(
            "CSV-файл успешно прочитан: %s строк, %s столбцов", len(df), len(df.columns) 
                   ) 
    except Exception as e: 
        logger.error("Ошибка при чтении CSV: %s", e) 
        raise

    # Загружаем данные:     
    try:
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)
        logger.info(f"Данные успешно загружены в БД ")
    except Exception as e:
        logger.error(f"Ошибка при загрузке в БД: {e}")

    # Закрываем соединения 
    engine.dispose()

if __name__ == "__main__":
    load_csv_to_table(
        csv_path="banks_data.csv",
        table_name="raw_banks_data",
        delimiter=',',
        encoding="utf-8",
        if_exists='replace'   
    )
    logger.info(f'Данные загружены в БД. Скрипт успешно завершил работу')   


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




