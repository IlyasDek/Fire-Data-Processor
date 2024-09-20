from crontab import CronTab
import datetime

def create_one_time_cron_job():
    cron = CronTab(user=True)
    now = datetime.datetime.now()
    run_time = now + datetime.timedelta(minutes=1)  # Задача запускается через 1 минуту

    # Используем полный путь к python3
    command = '/usr/local/bin/python3 /app/FirmsProcessing/Scripts/dwnld_firms.py >> /var/log/dwnld_firms.log 2>&1'
    job = cron.new(command=command, comment='one_time_job')
    job.setall(run_time.minute, run_time.hour, run_time.day, run_time.month, run_time.weekday())
    cron.write()
    print(f"Cron job created to run at {run_time}")

if __name__ == "__main__":
    create_one_time_cron_job()
