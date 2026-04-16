#!/usr/bin/env python
# coding: utf-8

# In[2]:


''' 
Скрипт выгружает данные ОКАТО по городам и регионам РФ из json файла ,
формирует pandas.DataFrame и загружает его в csv
Из csv формирует датафрейм по выбранным городам для дальнейщего анализа
''' 

import logging
import json
import pandas as pd
from typing import List, Dict, Optional


# Настройка логов
def setup_logging() -> logging.Logger:

    '''Настройка логирования.'''

    log_filename = 'okato_script.log'
    logger = logging.getLogger('okato_collector')
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



def get_okato()->pd.DataFrame:

    '''функция открывает и парсит JSON‑файл, приводит к датафрему'''

    with open('regions_all.json', 'r', encoding='utf-8') as file:
        data_json = json.load(file)
    logger.info('json файл прочитан')

    rows = []

    # Проходим по каждому региону
    for region in data_json:
        region_id = region['id']
        region_name = region['name']

        # Если у региона есть районы — обрабатываем их
        if 'districts' in region and region['districts']:
            for district in region['districts']:
                rows.append({
                'region_id': region_id,
                'region_name': region_name,
                'district_id': district['id'],
                'district_name': district['name']
                })
        else:
        # Если районов нет, создаём запись только для региона
            rows.append({
            'region_id': region_id,
            'region_name': region_name,
            'district_id': None,
            'district_name': None
            })

    # Создаём датафрейм
    okato = pd.DataFrame(rows)
    return okato

if __name__ == '__main__':
    try:
        okato = get_okato()
        okato.to_csv('okato.csv',index=False)
        logger.info(f'Загрузка okato csv завершена.')     
        logger.info('Скрипт успешно выполнен.')

    except Exception as e:
        logger.error(f'Ошибка при выполнении скрипта: {e}')


# In[ ]:




