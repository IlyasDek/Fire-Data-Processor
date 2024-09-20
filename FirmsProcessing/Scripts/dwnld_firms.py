import subprocess
import sys
from pathlib import Path
import logging
import os

# Настройка логирования
log_file = "/var/log/app.log"
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename=log_file)
logging.info('Starting download_firms_data script execution.')

# Путь к директории для скачивания данных внутри Docker контейнера
base_download_directory = Path("/app/FirmsProcessing/DownloadedData")

token = os.getenv('FIRMS_API_TOKEN')
if not token:
    logging.error("Token is not set.")

urls = {
    "MODIS_C6": "https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_Russia_Asia_24h.csv",
    "SUOMI_NPP_VIIRS_C2": "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_Russia_Asia_24h.csv",
    "NOAA_20_VIIRS_C2": "https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/csv/J1_VIIRS_C2_Russia_Asia_24h.csv",
    "NOAA_21_VIIRS_C2": "https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-21-viirs-c2/csv/J2_VIIRS_C2_Russia_Asia_24h.csv"
}

def download_data():
    logging.info("Начало процесса скачивания данных.")
    try:
        # Проверка наличия базовой директории для загрузки
        if not base_download_directory.exists():
            logging.info(f"Создание базовой директории для загрузки данных: {base_download_directory}")
            base_download_directory.mkdir(parents=True, exist_ok=True)
        
        for satellite, url in urls.items():
            satellite_directory = base_download_directory / satellite
            if not satellite_directory.exists():
                logging.info(f"Создание директории для {satellite}: {satellite_directory}")
                satellite_directory.mkdir(parents=True, exist_ok=True)
            
            logging.info(f"Запуск процесса wget для скачивания данных с {url}.")
            subprocess.run([
                "wget", "-e", "robots=off", "-m", "-np", "-R", ".html,.tmp", "-nH", "--cut-dirs=4",
                url, "--header", f"Authorization: Bearer {token}", "-P", str(satellite_directory)],
                check=True)
        
        logging.info("Скачивание завершено успешно.")
        
        logging.info("Запуск process_data.py для обработки данных.")
        subprocess.run(["/usr/local/bin/python3", "/app/FirmsProcessing/Scripts/process_data.py"], check=True)
        logging.info("Обработка данных завершена успешно.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Ошибка при скачивании файлов или обработке данных: {e}")
    except Exception as e:
        logging.error(f"Произошла ошибка: {e}")

if __name__ == "__main__":
    download_data()

logging.info('Finished download_firms_data script execution.')
