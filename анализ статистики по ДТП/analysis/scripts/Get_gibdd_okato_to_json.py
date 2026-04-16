'''
Получение ОКАТО код с сайта ГИБДД в формате json 
'''

import requests
import json
import time
from datetime import datetime
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gibdd_okato.log', mode='a', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger()

#дата всегда прошлый месяц
def get_all_regions():
    now = datetime.now()
    year = now.year
    month = now.month - 1 if now.month > 1 else 12
    if month == 12:
        year -= 1
    
    logger.info("Получаем список регионов РФ...")  
    rf_payload = {
        "maptype": 1,
        "region": "877", 
        "date": f'["MONTHS:{month}.{year}"]',
        "pok": "1"
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.post(
            "http://stat.gibdd.ru/map/getMainMapData",
            json=rf_payload,
            headers=headers,
            timeout=30
        )
        
        if response.status_code != 200:
            logger.error(f"Ошибка: {response.status_code}")
            return
        
        result = response.json()
        metabase = json.loads(result["metabase"])
        maps_data = json.loads(metabase[0]["maps"])
        
        regions = []
        for region in maps_data:
            regions.append({
                "id": region["id"],
                "name": region["name"],
                "districts": []
            })
        
        logger.info(f"Найдено {len(regions)} регионов")
              
        for i, region in enumerate(regions, 1):
            print(f"[{i}/{len(regions)}] {region['name']} ({region['id']})...")
            
            region_payload = {
                "maptype": 1,
                "region": region["id"],
                "date": f'["MONTHS:{month}.{year}"]',
                "pok": "1"
            }
            
            try:
                reg_response = requests.post(
                    "http://stat.gibdd.ru/map/getMainMapData",
                    json=region_payload,
                    headers=headers,
                    timeout=30
                )
                
                if reg_response.status_code == 200:
                    reg_result = reg_response.json()
                    reg_metabase = json.loads(reg_result["metabase"])
                    reg_maps_data = json.loads(reg_metabase[0]["maps"])
                    
                    municipalities = []
                    for municipality in reg_maps_data:
                        municipalities.append({
                            "id": municipality["id"],
                            "name": municipality["name"]
                        })
                    
                    region["districts"] = municipalities
                    logger.info(f"найдено {len(municipalities)} муниципалитетов")
                else:
                    logger.error(f"ошибка {reg_response.status_code}")
                    
            except Exception as e:
                logger.error(f"ошибка: {e}")
            
            if i < len(regions):
                time.sleep(0.5)
        
        filename = "regions_all.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(regions, f, ensure_ascii=False, indent=2)
        
        total_municipalities = sum(len(r["districts"]) for r in regions)
        logger.info(f"Файл: {filename}")
        logger.info(f"Регионов: {len(regions)}")
        logger.info(f"Всего муниципалитетов: {total_municipalities}")
                
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")

if __name__ == "__main__":  
    get_all_regions()