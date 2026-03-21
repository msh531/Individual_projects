#!/usr/bin/env python
# coding: utf-8

# In[1]:


''' 
Скрипт выгрузки данных о ДТП по городам из API ГИБДД, за период 2015 - 2025 включительно  и их загрузки в БД. 
Скрипт скачивает данные по указанным городам РФ в JSON, парсит JSON, формирует pandas.DataFrame  
pandas.DataFrame загружает в БД (Supabase)
''' 

# импорт библиотек
import logging
import requests
import time
from datetime import datetime, timedelta
import pandas as pd
from typing import List, Dict, Optional
import json
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


# Настройка логов
def setup_logging() -> logging.Logger:

    '''Настройка логирования.'''

    log_filename = 'dtp_script.log'
    logger = logging.getLogger('dtp_collector')
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



def get_dtp_cards(region_id, district_id, year, month, start=1, end=100)-> Optional[List[Dict]]:

    '''Получение полных карточек ДТП с пагинацией'''

    url = "http://stat.gibdd.ru/map/getDTPCardData"

    payload = {
        "data": {
            "date": [f"MONTHS:{month}.{year}"],
            "ParReg": region_id,
            "order": {"type": "1", "fieldName": "dat"},
            "reg": district_id,
            "ind": "1",
            "st": str(start),
            "en": str(end),
            "fil": {
                "isSummary": False  # Полные данные вместо сводных
            },
            "fieldNames": [
                "dat", "time", "coordinates", "infoDtp", "k_ul", "dor", "ndu",
                "k_ts", "ts_info", "pdop", "pog", "osv", "s_pch", "s_pog",
                "n_p", "n_pg", "obst", "sdor", "t_osv", "t_p", "t_s", "v_p", "v_v"
            ]
        }
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    try:
        # Двойное кодирование JSON требуется API ГИБДД
        logger.debug(f'Запрос: region={region_id}, district={district_id}')
        request_data = {
            "data": json.dumps(payload["data"], separators=(',', ':'))
        }

        response = requests.post(
            url,
            json=request_data,
            headers=headers,
            timeout=30
        )

        if response.status_code == 200:
            response_data = json.loads(response.text)
            return json.loads(response_data["data"]).get("tab", [])
            logger.info(f'Получено {len(tab_data)} записей за {month}.{year}')
            return tab_data
        else:
            logger.error(f"Ошибка HTTP: {response.status_code}")
            return None

    except Exception as e:
        logger.error(f"Ошибка при запросе данных: {str(e)}")
        return None



# получаем окато по выбранным городам из csv файла
okato = pd.read_csv('okato.csv')
cities = okato.loc[okato['district_name'].isin(['г. Владивосток','г.Екатеринбург'])]

# Определяем временной промежуток
start_year = 2015
end_year = 2025

#Запуск скрипта
if __name__ == '__main__':
    # Словарь для хранения данных по годам
    dtp_data = {}

    logging.info(f'Обработка городов: {list(cities["district_name"])}')

    # Цикл по всем годам
    for year in range(start_year, end_year + 1):
        logging.info(f'Обработка года {year}')
        year_data = []  # Список для данных текущего года

        # Цикл по всем месяцам года
        for month in range(1, 13):
            logging.info(f"Запрос данных за {month}.{year}...")

            # Цикл по всем городам
            for _, city in cities.iterrows():
                monthly_data = get_dtp_cards(
                    region_id=city['region_id'],
                    district_id=city['district_id'],
                    year=year,
                    month=month
                )
                if monthly_data is not None and len(monthly_data) > 0:
                # Добавляем метаданные к каждой записи
                    for record in monthly_data:
                        record.update({
                        'region_id': city['region_id'],
                        'region_name': city['region_name'],
                        'district_id': city['district_id'],
                        'district_name': city['district_name'],
                        'year': year,
                        'month': month
                        })

                    year_data.extend(monthly_data)
                    logging.info(f"получено {len(monthly_data)} записей")
                else:
                    logging.error("данные отсутствуют или ошибка запроса")

        # Сохраняем данные за год в общий словарь
        dtp_data[year] = year_data
        logging.info(f"Год {year} обработан. Всего записей: {len(year_data)}")

    logging.info(f"Обработано лет: {len(dtp_data)}")


    # 1. Основной DataFrame с данными о ДТП (только верхний уровень)
    main_data = []
    for year in range(2015,2026):
        for item in dtp_data[year]:
            main_item = {
                'KartId': item.get('KartId'),
                'rowNum': item.get('rowNum'),
                'date': item.get('date'),
                'Time': item.get('Time'),
                'District': item.get('District'),
                'DTP_V': item.get('DTP_V'),
                'POG': item.get('POG'),
                'RAN': item.get('RAN'),
                'K_TS': item.get('K_TS'),
                'K_UCH': item.get('K_UCH'),
                'emtp_number': item.get('emtp_number')
            }
            main_data.append(main_item)

    # Сохраняем DataFrame в словарь с ключом‑годом
    main_df = pd.DataFrame(main_data)

    # 2. DataFrame с общей информацией о ДТП (infoDtp)
    info_dtp_list = []
    for year in range(2015,2026):
        for item in dtp_data[year]:
            info_row = {
            'KartId': item['KartId'],
            'n_p': item['infoDtp'].get('n_p', ''),
            'street': item['infoDtp'].get('street', ''),
            'house': item['infoDtp'].get('house', ''),
            'dor': item['infoDtp'].get('dor', ''),
            'km': item['infoDtp'].get('km', ''),
            'm': item['infoDtp'].get('m', ''),
            'k_ul': item['infoDtp'].get('k_ul', ''),
            'dor_k': item['infoDtp'].get('dor_k', ''),
            'dor_z': item['infoDtp'].get('dor_z', ''),
            's_pch': item['infoDtp'].get('s_pch', ''),
            'osv': item['infoDtp'].get('osv', ''),
            'change_org_motion': item['infoDtp'].get('change_org_motion', ''),
            's_dtp': item['infoDtp'].get('s_dtp', ''),
            'COORD_W': item['infoDtp'].get('COORD_W', ''),
            'COORD_L': item['infoDtp'].get('COORD_L', ''),
            # Преобразуем списки в строки
            'ndu': ', '.join(item['infoDtp'].get('ndu', [])) if item['infoDtp'].get('ndu') else '',
            'sdor': ', '.join(item['infoDtp'].get('sdor', [])) if item['infoDtp'].get('sdor') else '',
            'factor': ', '.join(item['infoDtp'].get('factor', [])) if item['infoDtp'].get('factor') else '',
            's_pog': ', '.join(item['infoDtp'].get('s_pog', [])) if item['infoDtp'].get('s_pog') else '',
            'OBJ_DTP': ', '.join(item['infoDtp'].get('OBJ_DTP', [])) if item['infoDtp'].get('OBJ_DTP') else ''
        }
            info_dtp_list.append(info_row)
        info_dtp_df = pd.DataFrame(info_dtp_list)

    # 3. DataFrame с информацией о транспортных средствах
    ts_data = []
    for year in range(2015,2026):
        for item in dtp_data[year]:
            kart_id = item['KartId']
            if 'ts_info' in item['infoDtp']:
                for ts in item['infoDtp']['ts_info']:
                    ts_row = {
                    'KartId': kart_id,
                    'n_ts': ts.get('n_ts', ''),
                    'ts_s': ts.get('ts_s', ''),
                    't_ts': ts.get('t_ts', ''),
                    'marka_ts': ts.get('marka_ts', ''),
                    'm_ts': ts.get('m_ts', ''),
                    'color': ts.get('color', ''),
                    'r_rul': ts.get('r_rul', ''),
                    'g_v': ts.get('g_v', ''),
                    'm_pov': ts.get('m_pov', ''),
                    't_n': ts.get('t_n', ''),
                    'f_sob': ts.get('f_sob', ''),
                    'o_pf': ts.get('o_pf', '')
                }
                    ts_data.append(ts_row)

    ts_df = pd.DataFrame(ts_data)

    # 4. DataFrame с участниками ДТП (из ts_uch)
    uch_data = []
    for year in range(2015,2026):
        for item in dtp_data[year]:
            kart_id = item['KartId']
            if 'ts_info' in item['infoDtp']:
                for ts in item['infoDtp']['ts_info']:
                    n_ts = ts.get('n_ts', '')
                    if 'ts_uch' in ts and ts['ts_uch']:
                        for uch in ts['ts_uch']:
                            uch_row = {
                            'KartId': kart_id,
                            'n_ts': n_ts,
                            'K_UCH': uch.get('K_UCH', ''),
                            'S_T': uch.get('S_T', ''),
                            'POL': uch.get('POL', ''),
                            'V_ST': uch.get('V_ST', ''),
                            'ALCO': uch.get('ALCO', ''),
                            'SAFETY_BELT': uch.get('SAFETY_BELT', ''),
                            'S_SM': uch.get('S_SM', ''),
                            'N_UCH': uch.get('N_UCH', ''),
                            'S_SEAT_GROUP': uch.get('S_SEAT_GROUP', ''),
                            'INJURED_CARD_ID': uch.get('INJURED_CARD_ID', ''),
                            # Преобразуем списки нарушений в строки
                            'NPDD': ', '.join(uch.get('NPDD', [])) if uch.get('NPDD') else '',
                            'SOP_NPDD': ', '.join(uch.get('SOP_NPDD', [])) if uch.get('SOP_NPDD') else ''
                        }
                            uch_data.append(uch_row)

    uch_data_df = pd.DataFrame(uch_data)

    # 5. DataFrame с дополнительными участниками (из uchInfo)
    uch_info_data = []
    for year in range(2015,2026):
        for item in dtp_data[year]:
            kart_id = item['KartId']
            if 'uchInfo' in item and item['uchInfo']:
                for uch in item['uchInfo']:
                    uch_row = {
                    'KartId': kart_id,
                    'n_ts': 'N/A',  # Для дополнительных участников нет привязки к ТС
                    'K_UCH': uch.get('K_UCH', ''),
                    'S_T': uch.get('S_T', ''),
                    'POL': uch.get('POL', ''),
                    'V_ST': uch.get('V_ST', ''),
                    'ALCO': uch.get('ALCO', ''),
                    'SAFETY_BELT': 'N/A',  # Для дополнительных участников нет этой информации
                    'S_SM': uch.get('S_SM', ''),
                    'N_UCH': uch.get('N_UCH', ''),
                    'S_SEAT_GROUP': 'N/A',
                    'INJURED_CARD_ID': 'N/A',
                    # Преобразуем списки нарушений в строки
                    'NPDD': ', '.join(uch.get('NPDD', [])) if uch.get('NPDD') else '',
                    'SOP_NPDD': ', '.join(uch.get('SOP_NPDD', [])) if uch.get('SOP_NPDD') else ''
                }
                    uch_info_data.append(uch_row)
    uch_data_df.head(10)

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
        except Exception as e:
                logger.error(f'Ошибка при подключении к базе: {e}')
                raise
        return engine

    engine = connection_to_DB()
    connection_to_DB()
    logger.info(f'Подключение к базе успешно')

    try:
        logger.info(f'Загружаем все данные в базу')

        main_df.to_sql('main_df', con=engine, if_exists='replace')
        logger.info(f'Загрузка main_df успешна')

        info_dtp_df.to_sql('info_dtp_df', con=engine, if_exists='replace')
        logger.info(f'Загрузка info_dtp_df успешна')

        ts_df.to_sql('ts_df', con=engine, if_exists='replace')
        logger.info(f'Загрузка ts_df успешна ')

        uch_data_df.to_sql('uch_data_df', con=engine, if_exists='replace')
        logger.info(f'Загрузка uch_data_df успешна ')


    except Exception as e:
        logger.error(f'Ошибка при выполнении скрипта: {e}')
    logger.info('Скрипт успешно выполнен.')































































































