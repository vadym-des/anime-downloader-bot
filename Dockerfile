# Офіційний ультралегкий Python образ
FROM python:3.12-slim

WORKDIR /app

# Оновлюємо систему та встановлюємо curl для healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копіюємо всі необхідні скрипти бота
COPY . .

# Директорія для зберігання бази даних SQLite
RUN mkdir -p /app/data

# Змінні середовища за замовчуванням
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

EXPOSE 8080

# Запуск хмарного бота-диспетчера
CMD ["python", "bot.py"]
