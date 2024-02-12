"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv

import psycopg2

"""Создаем подключение к БД"""

conn = psycopg2.connect(
    host='localhost',
    database='north',
    user='postgres',
    password='123456'
)

"""Создаем курсор"""
cur = conn.cursor()

try:
    with open('north_data/employees_data.csv', newline='') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            query = 'INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)'
            cur.execute(query, row)

    with open('north_data/customers_data.csv', newline='') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            query = 'INSERT INTO customers VALUES (%s, %s, %s)'
            cur.execute(query, row)

    with open('north_data/orders_data.csv', newline='') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            query = 'INSERT INTO orders VALUES (%s, %s, %s, %s, %s)'
            cur.execute(query, row)

    conn.commit()

    cur.execute('SELECT * FROM employees')
    print(cur.fetchall())

    cur.execute('SELECT * FROM customers')
    print(cur.fetchall())

    cur.execute('SELECT * FROM orders')
    print(cur.fetchall())

finally:
    cur.close()
    conn.close()
