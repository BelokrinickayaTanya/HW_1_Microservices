import time
import pandas as pd
import matplotlib.pyplot as plt
import os


def main():
    print("Plot сервис запущен. Мониторинг логов...")

    last_processed_id = None

    while True:
        try:
            # Читаем CSV файл
            if os.path.exists('logs/metric_log.csv'):
                df = pd.read_csv('logs/metric_log.csv')

                # Проверяем, есть ли новые данные
                if not df.empty and (last_processed_id is None or
                                     df['id'].iloc[-1] != last_processed_id):
                    # Строим гистограмму абсолютных ошибок
                    plt.figure(figsize=(10, 6))
                    plt.hist(df['absolute_error'], bins=20, alpha=0.7,
                             color='blue', edgecolor='black')

                    plt.title('Распределение абсолютных ошибок модели')
                    plt.xlabel('Абсолютная ошибка')
                    plt.ylabel('Частота')
                    plt.grid(True, alpha=0.3)

                    # Добавляем статистику
                    mean_error = df['absolute_error'].mean()
                    median_error = df['absolute_error'].median()
                    plt.axvline(mean_error, color='red', linestyle='--',
                                label=f'Среднее: {mean_error:.2f}')
                    plt.axvline(median_error, color='green', linestyle='--',
                                label=f'Медиана: {median_error:.2f}')

                    plt.legend()

                    # Сохраняем график
                    plt.savefig('logs/error_distribution.png',
                                dpi=150, bbox_inches='tight')
                    plt.close()

                    last_processed_id = df['id'].iloc[-1]
                    print(f"График обновлен. Обработано записей: {len(df)}")
                    print(f"Средняя ошибка: {mean_error:.2f}")
                    print(f"Медианная ошибка: {median_error:.2f}")
                    print("-" * 50)

        except Exception as e:
            print(f"Ошибка при обработке данных: {e}")

        # Проверяем каждые 2 секунды
        time.sleep(2)


if __name__ == '__main__':
    main()