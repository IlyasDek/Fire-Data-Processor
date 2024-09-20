import csv
import os
import psycopg2
from datetime import datetime, timedelta
from pathlib import Path
import logging
import subprocess
from decimal import Decimal

# Настройка логирования
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', filename='/var/log/app.log')
logging.info('Starting data processing script execution.')

download_directory = "/app/FirmsProcessing/DownloadedData"
processed_directory = "/app/FirmsProcessing/ProcessedData"

db_config = {
    "host": os.getenv('DB_HOST', 'localhost'),
    "dbname": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD')
}

# Глобальные переменные для счетчиков
total_points_all_files = 0
points_within_kazakhstan_all_files = 0
added_points_all_files = 0
updated_points_all_files = 0
total_files_processed = 0

def is_within_kazakhstan(lat, lon, cur):
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM boundaries 
            WHERE ST_Contains(geom, ST_GeomFromText('POINT(%s %s)', 4326))
        )
    """, (lon, lat))
    return cur.fetchone()[0]

def get_forests_for_point(lat, lon, cur):
    cur.execute("""
        SELECT forestry_id FROM forestry_geometries 
        WHERE ST_Contains(geom, ST_GeomFromText('POINT(%s %s)', 4326))
    """, (lon, lat))
    return [row[0] for row in cur.fetchall()]

def create_temp_table_for_current_points(cur, current_points):
    try:
        cur.execute("CREATE TEMP TABLE current_points (latitude DOUBLE PRECISION, longitude DOUBLE PRECISION, acq_date DATE, acq_time TIME)")
        insert_query = "INSERT INTO current_points (latitude, longitude, acq_date, acq_time) VALUES (%s, %s, %s, %s)"
        cur.executemany(insert_query, list(current_points))
    except Exception as e:
        logging.error(f"Error creating or populating temporary table 'current_points': {e}")
        raise

def insert_or_update_db(row, cur, conn, satellite, added_points, updated_points):
    try:
        lat, lon = Decimal(row[0]), Decimal(row[1])
        acq_date = datetime.strptime(row[5], "%Y-%m-%d").date()
        acq_time = datetime.strptime(row[6], "%H%M").time()
        local_time = (datetime.combine(acq_date, acq_time) + timedelta(hours=5)).time()
        brightness, scan, track = Decimal(row[2]), Decimal(row[3]), Decimal(row[4])
        confidence = row[8]
        version = row[9]
        bright_t31 = Decimal(row[10]) if row[10] else None
        frp = Decimal(row[11]) if row[11] else None
        daynight = row[12]

        
        cur.execute("""
            SELECT id, satellite FROM fires 
            WHERE latitude = %s AND longitude = %s AND acq_date = %s AND acq_time = %s
        """, (lat, lon, acq_date, acq_time))
        result = cur.fetchone()

        if result:
            fire_id, existing_satellites = result
            if satellite not in existing_satellites.split(','):
                updated_satellites = existing_satellites + ',' + satellite
                cur.execute("""
                    UPDATE fires 
                    SET satellite = %s 
                    WHERE id = %s
                """, (updated_satellites, fire_id))
                updated_points += 1
        else:
            query = """
                INSERT INTO fires (latitude, longitude, brightness, scan, track, acq_date, acq_time, local_time, satellite, confidence, version, bright_t31, frp, daynight, geom) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText('POINT(%s %s)', 4326)) 
                RETURNING id
            """
            values = (lat, lon, brightness, scan, track, acq_date, acq_time, local_time, satellite, confidence, version,
                      bright_t31, frp, daynight, lon, lat)

            
            cur.execute(query, values)
            fire_id = cur.fetchone()[0]

            forests = get_forests_for_point(lat, lon, cur)
            if forests:
                for forest_id in forests:
                    cur.execute("""
                        INSERT INTO fire_forest_relations (fire_id, forestry_id) 
                        VALUES (%s, %s)
                    """, (fire_id, forest_id))
            added_points += 1
        conn.commit()
    except Exception as e:
        logging.error(f"Error inserting or updating point: {e}")
        conn.rollback()
    return added_points, updated_points
    
    
def process_file(file_path, cur, conn, satellite, current_points):
    global total_points_all_files, points_within_kazakhstan_all_files, added_points_all_files, updated_points_all_files
    total_points = 0
    points_within_kazakhstan = 0
    added_points = 0
    updated_points = 0
    try:
        logging.info(f"Processing file: {file_path}")
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            header = next(reader, None)
            if not header:
                logging.error(f"No data in file: {file_path}")
                return
            for row in reader:
                total_points += 1
                lat, lon = float(row[0]), float(row[1])
                acq_date = row[5]
                acq_time = row[6]
                current_points.add((lat, lon, acq_date, acq_time))
                if is_within_kazakhstan(lat, lon, cur):
                    points_within_kazakhstan += 1
                    added_points, updated_points = insert_or_update_db(row, cur, conn, satellite, added_points, updated_points)
        total_points_all_files += total_points
        points_within_kazakhstan_all_files += points_within_kazakhstan
        added_points_all_files += added_points
        updated_points_all_files += updated_points
        logging.info(f"Finished processing file: {file_path}")
    except Exception as e:
        logging.error(f"Failed to process file {file_path}: {e}")
        conn.rollback()

def process_data():
    global total_files_processed
    current_points = set()
    with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
        for satellite in os.listdir(download_directory):
            satellite_directory = os.path.join(download_directory, satellite)
            if os.path.isdir(satellite_directory):
                for root, _, files in os.walk(satellite_directory):
                    for file_name in files:
                        if file_name.endswith('.csv'):
                            logging.info(f"Found file: {file_name}")
                            process_file(Path(root) / file_name, cur, conn, satellite, current_points)
                            total_files_processed += 1

        create_temp_table_for_current_points(cur, current_points)
        logging.info(f"Temporary table created with {len(current_points)} points.")
        conn.commit()

if __name__ == "__main__":
    process_data()
    logging.info('Finished processing all files.')
    logging.info(f'Total files processed: {total_files_processed}')
    logging.info(f'Total points processed: {total_points_all_files}')
    logging.info(f'Total points within Kazakhstan: {points_within_kazakhstan_all_files}')
    logging.info(f'Total points added to database: {added_points_all_files}')
    logging.info(f'Total points updated in database: {updated_points_all_files}')

    # Запуск второго скрипта для архивирования данных
    subprocess.run(["/usr/local/bin/python3", "/app/FirmsProcessing/Scripts/archive_data.py"], check=True)

logging.info('Finished process_data script execution.')
