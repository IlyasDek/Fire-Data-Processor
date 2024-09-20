import logging
from datetime import datetime, timedelta
import re
import os
from crontab import CronTab

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='/var/log/app.log')
logging.info('Starting generate_cron script execution.')

DAILY_JOB_COMMENT = "generate_cron_job"
WEEKLY_LOG_CLEAN_COMMENT = "weekly_log_clean_job"
PYTHON_PATH = "/usr/local/bin/python3"

def manage_cron_jobs(cron_jobs, cron):
    existing_jobs = {job.comment: job for job in cron}
    updated_jobs = set()

    for job_string in cron_jobs:
        command, time_string, comment = job_string.split("|||")
        if comment in existing_jobs:
            job = existing_jobs[comment]
            if job.command != command or str(job.slices) != time_string:
                job.set_command(command)
                job.setall(time_string)
                logging.info(f"Updated cron job: {comment}")
            updated_jobs.add(comment)
        else:
            task = cron.new(command=command, comment=comment)
            task.setall(time_string)
            logging.info(f"Added new cron job: {comment} with time {time_string}")
            updated_jobs.add(comment)

    # Удаляем только те задачи, которые не являются ежедневными задачами generate_cron
    for comment in set(existing_jobs) - updated_jobs:
        if comment not in [DAILY_JOB_COMMENT, WEEKLY_LOG_CLEAN_COMMENT]:
            existing_jobs[comment].delete()
            logging.info(f"Removed outdated cron job: {comment}")

    cron.write()

def generate_cron_jobs(passlist_path, script_path, cron):
    if not os.path.exists(passlist_path):
        logging.error(f"Passlist file {passlist_path} does not exist.")
        return
    if not os.path.exists(script_path):
        logging.error(f"Script file {script_path} does not exist.")
        return

    cron_jobs = []
    date_pattern = re.compile(r"\d{2} \w{3} \d{4}")

    try:
        with open(passlist_path, 'r') as file:
            lines = file.readlines()
            for line in lines:
                line = line.strip()
                logging.info(f"Processing line: {line}")
                parts = re.split(r'\t+', line)
                if len(parts) >= 2 and date_pattern.match(parts[0]):
                    date_str = parts[0]
                    time_str = parts[1]
                    logging.info(f"Parsed date: {date_str}, time: {time_str}")
                    try:
                        pass_datetime = datetime.strptime(f"{date_str} {time_str}", "%d %b %Y %H:%M:%S")
                    except ValueError as e:
                        logging.error(f"Date parsing error: {e}")
                        continue
                    scheduled_time = pass_datetime + timedelta(hours=3)
                    cron_job_command = f"{PYTHON_PATH} {script_path} >> /var/log/app.log 2>&1"
                    cron_job_time = f"{scheduled_time.minute} {scheduled_time.hour} {scheduled_time.day} {scheduled_time.month} *"
                    comment = f"job_{hash(line)}"
                    cron_job = f"{cron_job_command}|||{cron_job_time}|||{comment}"
                    cron_jobs.append(cron_job)
                    logging.info(f"Generated cron job: {comment} with time {cron_job_time}")
                else:
                    logging.warning(f"Line skipped due to incorrect format: {line}")
    except Exception as e:
        logging.error(f"Error processing PassList.txt: {e}")

    manage_cron_jobs(cron_jobs, cron)

def main():
    logging.info(f"Environment variables: {os.environ}")
    passlist_path = "/app/FirmsProcessing/PassList.txt"
    script_path = "/app/FirmsProcessing/Scripts/dwnld_firms.py"
    cron = CronTab(user=True)  
    logging.info("Starting cron job generation.")
    generate_cron_jobs(passlist_path, script_path, cron)

    # Добавляем ежедневную задачу generate_cron
    daily_job_exists = any(job.comment == DAILY_JOB_COMMENT for job in cron)
    if not daily_job_exists:
        task = cron.new(command=f"{PYTHON_PATH} /app/FirmsProcessing/Scripts/generate_cron.py >> /var/log/app.log 2>&1", comment=DAILY_JOB_COMMENT)
        task.setall("0 0 * * *")
        cron.write()
        logging.info(f"Added daily cron job: {DAILY_JOB_COMMENT}")

    # Добавляем еженедельную задачу очистки логов
    weekly_log_clean_exists = any(job.comment == WEEKLY_LOG_CLEAN_COMMENT for job in cron)
    if not weekly_log_clean_exists:
        task = cron.new(command="echo '' > /var/log/app.log", comment=WEEKLY_LOG_CLEAN_COMMENT)
        task.setall("0 0 * * 0")
        cron.write()
        logging.info(f"Added weekly log clean job: {WEEKLY_LOG_CLEAN_COMMENT}")

    logging.info("Current cron jobs:")
    for job in cron:
        logging.info(f"Cron job: {job}")

if __name__ == "__main__":
    main()

logging.info('Finished generate_cron script execution.')
