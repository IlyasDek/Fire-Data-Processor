import psycopg2
from datetime import datetime, timedelta
import logging

# Настройка логирования
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', filename='/var/log/app.log')
logging.info('Starting archive script execution.')

db_config = {
    "host": os.getenv('DB_HOST', 'localhost'),
    "dbname": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD')
}

def archive_old_points(cur, cutoff_time):
    global archived_points_all_files
    logging.info(f"Cutoff time for archiving: {cutoff_time}")

    try:
        cur.execute("""
            SELECT id, latitude, longitude, acq_date, acq_time
            FROM fires
            WHERE acq_date + acq_time::interval < %s
        """, (cutoff_time,))
        
        points_to_archive = cur.fetchall()
        logging.info(f"Points to archive: {len(points_to_archive)}")

        if not points_to_archive:
            return

        archived_ids = []
        for point in points_to_archive:
            cur.execute("""
                INSERT INTO archived_fires (latitude, longitude, brightness, scan, track, acq_date, acq_time, local_time, satellite, confidence, version, bright_t31, frp, daynight, geom, archived_at)
                SELECT latitude, longitude, brightness, scan, track, acq_date, acq_time, local_time, satellite, confidence, version, bright_t31, frp, daynight, geom, NOW()
                FROM fires
                WHERE id = %s
                ON CONFLICT DO NOTHING
                RETURNING id
            """, (point[0],))
            
            new_archived_id = cur.fetchone()
            if new_archived_id:
                archived_ids.append(new_archived_id[0])
            else:
                cur.execute("""
                    SELECT id FROM archived_fires WHERE acq_date = %s AND acq_time = %s AND geom = ST_GeomFromText('POINT(%s %s)', 4326)
                """, (point[3], point[4], point[2], point[1]))
                existing_archived_id = cur.fetchone()
                if existing_archived_id:
                    archived_ids.append(existing_archived_id[0])

        archived_points = len(archived_ids)
        archived_points_all_files += archived_points

        logging.info(f"Archived {archived_points} points older than {cutoff_time}")
        logging.debug(f"Archived points IDs: {archived_ids}")

        if archived_points > 0:
            logging.info(f"Moving fire-forest relations for archived points")
            for old_id, new_id in zip(points_to_archive, archived_ids):
                cur.execute("""
                    INSERT INTO archived_fire_forest_relations (fire_id, forestry_id)
                    SELECT %s, forestry_id
                    FROM fire_forest_relations
                    WHERE fire_id = %s
                    ON CONFLICT DO NOTHING
                """, (new_id, old_id[0]))
            
            cur.execute("""
                DELETE FROM fire_forest_relations
                WHERE fire_id IN %s
            """, (tuple([point[0] for point in points_to_archive]),))

            cur.execute("""
                DELETE FROM fires
                WHERE id IN %s
            """, (tuple([point[0] for point in points_to_archive]),))
    except Exception as e:
        logging.error(f"Error archiving old points: {e}")
        cur.execute("ROLLBACK")

def archive_data():
    global archived_points_all_files
    archived_points_all_files = 0

    with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT MAX(acq_date + acq_time::interval) FROM fires
        """)
        max_acq_datetime = cur.fetchone()[0]

        if max_acq_datetime:
            cutoff_time = max_acq_datetime - timedelta(hours=24)
            archive_old_points(cur, cutoff_time)

        conn.commit()

if __name__ == "__main__":
    archive_data()
    logging.info('Finished archiving data.')
