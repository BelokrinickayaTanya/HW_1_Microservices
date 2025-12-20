import json
import csv
import pika
import pandas as pd
from collections import defaultdict
import os

rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')

class MetricService:
    def __init__(self):
        self.y_true_store = {}
        self.y_pred_store = {}

        # Инициализируем файл с заголовками
        log_file = 'logs/metric_log.csv'
        if not os.path.exists(log_file):
            with open(log_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['id', 'y_true', 'y_pred', 'absolute_error'])

        print("Metric сервис запущен...")

    def process_messages(self):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=rabbitmq_host))

        channel_y_true = connection.channel()
        channel_y_pred = connection.channel()

        channel_y_true.queue_declare(queue='y_true_queue')
        channel_y_pred.queue_declare(queue='y_pred_queue')

        def y_true_callback(ch, method, properties, body):
            message = json.loads(body)
            message_id = message['id']
            y_true = message['body']

            self.y_true_store[message_id] = y_true
            self.try_calculate_metric(message_id)

        def y_pred_callback(ch, method, properties, body):
            message = json.loads(body)
            message_id = message['id']
            y_pred = message['body']

            self.y_pred_store[message_id] = y_pred
            self.try_calculate_metric(message_id)

        channel_y_true.basic_consume(
            queue='y_true_queue',
            on_message_callback=y_true_callback,
            auto_ack=True
        )

        channel_y_pred.basic_consume(
            queue='y_pred_queue',
            on_message_callback=y_pred_callback,
            auto_ack=True
        )

        print("Ожидание сообщений...")

        # Запускаем потребление из обеих очередей
        channel_y_true.start_consuming()

        while True:
            connection.process_data_events()

    def try_calculate_metric(self, message_id):
        """Пытаемся вычислить метрику, если есть и y_true и y_pred"""
        if message_id in self.y_true_store and message_id in self.y_pred_store:
            y_true = self.y_true_store[message_id]
            y_pred = self.y_pred_store[message_id]

            # Вычисляем абсолютную ошибку
            absolute_error = abs(y_true - y_pred)

            # Записываем в CSV
            with open('logs/metric_log.csv', 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([message_id, y_true, y_pred, absolute_error])

            print(f"Записана метрика ID: {message_id}")
            print(f"y_true: {y_true}, y_pred: {y_pred}, error: {absolute_error}")
            print("-" * 50)

            # Удаляем обработанные данные (опционально)
            del self.y_true_store[message_id]
            del self.y_pred_store[message_id]


def main():
    service = MetricService()
    service.process_messages()


if __name__ == '__main__':
    main()