FROM python:3.11

WORKDIR /app

# Копируем зависимости и устанавливаем
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Создаем папки для логов
RUN mkdir -p logs

# Копируем все Python файлы
COPY *.py .

# Создаем точки входа для каждого сервиса
CMD ["python", "features.py"]