import json
import pickle
import pika
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import os

try:
    with open('data/california_data_train.pkl', 'rb') as f:
        data = pickle.load(f)
    # Получаем обучающие данные
    X_train = data['X_train']
    y_train = data['y_train']
    # target_names = data['target_names']

    print("Загружены данные из data/california_data_train.pkl")

except FileNotFoundError:
    print("Файл data/california_data_train.pkl не найден!")
    print("Запустите features.py для создания данных")
    exit()


model = RandomForestRegressor(n_estimators=10, max_depth=3, random_state=42)
model.fit(X_train, y_train)

print("Модель обучена. Готов к предсказаниям...")

rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')

def main():

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbitmq_host))
    channel = connection.channel()

    # Создаем очереди
    channel.queue_declare(queue='X_queue')
    channel.queue_declare(queue='y_pred_queue')

    def callback(ch, method, properties, body):
        message = json.loads(body)
        message_id = message['id']
        features = np.array(message['features']).reshape(1, -1)

        # Делаем предсказание на тестовых данных которые пришли из features.py
        prediction = model.predict(features)[0]


        # Формируем ответ
        response = {
            'id': message_id,
            'body': float(prediction),

        }

        # Отправляем предсказание
        channel.basic_publish(
            exchange='',
            routing_key='y_pred_queue',
            body=json.dumps(response)
        )

        print(f"Получены тестовые признаки ID: {message_id}")
        print(f"Предсказание: {prediction}")


    channel.basic_consume(
        queue='X_queue',
        on_message_callback=callback,
        auto_ack=True
    )

    print("Model сервис запущен. Ожидание сообщений...")
    channel.start_consuming()


if __name__ == '__main__':
    main()