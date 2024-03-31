"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv
import os

bd = ['employees_data.csv', 'customers_data.csv', 'orders_data.csv']
bd_name = ['employees', 'customers', 'orders']


def add_data_in_bd(bd, bd_name):
	"""Скрипт для заполнения данными таблиц в БД Postgres."""
	password = os.environ.get('BD_PASSWORD')
	conn = psycopg2.connect(host='localhost', database='north', user='postgres', password=password)
	cur = conn.cursor()
	for i in range(len(bd)):
		with open(os.path.join('north_data', bd[i]), 'r') as csvfile:
			header = next(csvfile)
			csvreader = csv.reader(csvfile)
			for row in csvreader:
				values = '%s, ' * len(row)
				cur.execute(f"INSERT INTO {bd_name[i]} VALUES ({values[:-2]})", row)
			conn.commit()
	cur.close()
	conn.close()


if __name__ == '__main__':
	add_data_in_bd(bd, bd_name)