import time
import json
import random
from datetime import datetime
import pandas as pd
import pika
from sklearn.model_selection import train_test_split
import numpy as np
import pickle
import os

from sklearn.datasets import fetch_california_housing
california = fetch_california_housing()
X = california.data
y = california.target

# РАЗДЕЛЯЕМ ДАННЫЕ НА ОБУЧАЮЩИЕ И ТЕСТОВЫЕ
X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.3,  # 30% на тестирование
    random_state=42  # для воспроизводимости
)

print("Данные разделены:")
print(f"Размер обучающей выборки: {len(X_train)} записей")
print(f"Размер тестовой выборки: {len(X_test)} записей")

# Сохраняем train данные в отдельный pickle файл для модели.
data_to_save_train = {
    'X_train' : X_train,
    'y_train' : y_train,
    'feature_names' : california.feature_names,
    'target_names' : california.target_names
}

with open ('data/california_data_train.pkl', 'wb') as f:
    pickle.dump(data_to_save_train, f)
print("Данные сохранены в data/california_data_train.pkl")


# Используем тестовые данные для отправки в очереди
X = X_test
y = y_test

rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')

def main():

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbitmq_host))
    channel = connection.channel()

    # Создаем очереди
    channel.queue_declare(queue='X_queue')
    channel.queue_declare(queue='y_true_queue')


    print("Features сервис запущен")
    print(f"Отправка тестовых данных ({len(X)} записей)")


    while True:
        # Выбираем случайную строку из тестовой выборки
        random_row = random.randint(0, len(X) - 1)

        # Создаем уникальный ID
        message_id = datetime.timestamp(datetime.now())

        # Формируем сообщение с признаками
        features_message = {
            'id': message_id,
            'features': X[random_row].tolist(),
            'row_index': random_row  # для отладки
        }

        # Формируем сообщение с истинным ответом
        y_true_message = {
            'id': message_id,
            'body': float(y[random_row])
        }

        # Отправляем в очереди
        channel.basic_publish(
            exchange='',
            routing_key='X_queue',
            body=json.dumps(features_message)
        )

        channel.basic_publish(
            exchange='',
            routing_key='y_true_queue',
            body=json.dumps(y_true_message)
        )

        print(f" Отправлены тестовые данные #{random_row}")
        print(f" ID: {message_id}")
        print(f" Признаки: {features_message['features']}")
        print(f" Цена: {y_true_message['body']:.2f} (тыс. долл.)")


        # Задержка
        time.sleep(10)


if __name__ == '__main__':
    main()