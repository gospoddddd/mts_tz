import requests
import psycopg2
from psycopg2 import sql
import json
import time

# Параметры подключения к базе данных
DB_NAME = "home"
DB_USER = "etl_user"
DB_PASSWORD = "O=g8:\\12foWo%Z7"
DB_HOST = "localhost"
DB_PORT = "5432"

# Функция для получения данных из API
def fetch_metro_data():
    response = requests.get("https://api.hh.ru/metro")
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("Ошибка при получении данных из API")

# Функция для обновления данных в базе данных
def update_database(data):
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, options='-c client_encoding=utf8')
    cursor = conn.cursor()

    # Очистка таблиц перед обновлением
    cursor.execute("DELETE FROM work.metro_stations")
    cursor.execute("DELETE FROM work.metro_lines")

    # Вставка новых данных
    for city in data:
        city_name = city['name']
        for line in city['lines']:
            line_name = line['name']
            cursor.execute(
                sql.SQL("INSERT INTO work.metro_lines (city, line_name) VALUES (%s, %s)"),
                [city_name, line_name]
            )
            for station in line['stations']:
                station_name = station['name']
                cursor.execute(
                    sql.SQL("INSERT INTO work.metro_stations (city, line_name, station_name) VALUES (%s, %s, %s)"),
                    [city_name, line_name, station_name]
                )

    conn.commit()
    cursor.close()
    conn.close()

# Основная функция

def main():
    while True:
        try:
            data = fetch_metro_data()
            update_database(data)
            print("Данные успешно обновлены")
        except Exception as e:
            print(f"Произошла ошибка: {e}")
        time.sleep(3600)  # Обновление данных каждый час

if __name__ == "__main__":
    main()
