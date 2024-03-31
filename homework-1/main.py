import psycopg2
import csv
import os

bd = ['employees_data.csv', 'customers_data.csv', 'orders_data.csv']
bd_name = ['employees', 'customers', 'orders']


def add_data_in_bd(bd, bd_name):
    """Скрипт для заполнения данными таблиц в БД Postgres."""
    try:
        conn = psycopg2.connect(host='localhost', database='north', user='Baxtiyor', password='18071990')
        cur = conn.cursor()
        for i in range(len(bd)):
            with open(os.path.join('north_data', bd[i]), 'r') as csvfile:
                header = next(csvfile)
                csvreader = csv.reader(csvfile)
                for row in csvreader:
                    placeholders = ', '.join(['%s'] * len(row))
                    query = f"INSERT INTO {bd_name[i]} VALUES ({placeholders})"
                    cur.execute(query, row)
                conn.commit()
    except psycopg2.Error as e:
        print("Error executing SQL query:", e)
    finally:
        if conn is not None:
            cur.close()
            conn.close()


if __name__ == '__main__':
    add_data_in_bd(bd, bd_name)
