# Используем официальный образ Python 3.8
FROM python:3.8-slim

# Устанавливаем рабочую директорию в контейнере
WORKDIR /app

# Установка cron, wget, PostgreSQL клиента и необходимых библиотек Python
RUN apt-get update && \
    apt-get install -y cron wget postgresql-client gcc libpq-dev && \
    pip install --no-cache-dir python-crontab psycopg2-binary

# Копируем директорию FirmsProcessing в контейнер
COPY FirmsProcessing /app/FirmsProcessing

# Создаем директории для хранения временных файлов и данных
RUN mkdir -p /app/FirmsProcessing/DownloadedData /app/FirmsProcessing/ProcessedData

# Даем права на выполнение скриптов с проверкой существования файлов
RUN ls /app/FirmsProcessing/Scripts/ && chmod +x /app/FirmsProcessing/Scripts/*.py

# Создание cron-задания для запуска dwnld_firms.py каждые 15 минут и для очистки логов каждую неделю
RUN echo "*/15 * * * * /usr/local/bin/python3 /app/FirmsProcessing/Scripts/dwnld_firms.py >> /var/log/dwnld_firms.log 2>&1" > /etc/cron.d/my_cron_jobs
RUN echo "0 0 * * 0 truncate -s 0 /var/log/dwnld_firms.log" >> /etc/cron.d/my_cron_jobs
RUN echo "0 0 * * 0 truncate -s 0 /var/log/app.log" >> /etc/cron.d/my_cron_jobs
RUN echo "0 0 * * 0 truncate -s 0 /var/log/cron.log" >> /etc/cron.d/my_cron_jobs

# Устанавливаем правильные права на файл crontab
RUN chmod 0644 /etc/cron.d/my_cron_jobs

# Применение cron-заданий
RUN crontab /etc/cron.d/my_cron_jobs

# Создание пустых файлов логов
RUN touch /var/log/cron.log /var/log/app.log /var/log/dwnld_firms.log

# Запуск cron и просмотр логов
CMD ["sh", "-c", "cron && tail -f /var/log/cron.log /var/log/app.log /var/log/dwnld_firms.log"]
