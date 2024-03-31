import json
import psycopg2
from config import config


def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'
    db_name = 'my_new_db'

    params = config()
    conn = None

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur, json_file)
                print(f"FOREIGN KEY успешно добавлены")

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    try:
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE {db_name}")
        cur.close()
        conn.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    try:
        with open(script_file, 'r') as f:
            script = f.read()
            cur.execute(script)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def create_suppliers_table(cur) -> None:
    """Создает таблицу suppliers."""
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS suppliers (
                supplier_id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                contact_name VARCHAR(255),
                city VARCHAR(255),
                country VARCHAR(255)
            )
        """)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    try:
        with open(json_file, 'r') as f:
            suppliers_data = json.load(f)
        return suppliers_data
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def insert_suppliers_data(cur, suppliers: list[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    try:
        for supplier in suppliers:
            cur.execute("""
                INSERT INTO suppliers (name, contact_name, city, country)
                VALUES (%s, %s, %s, %s)
            """, (supplier['name'], supplier['contact_name'], supplier['city'], supplier['country']))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def add_foreign_keys(cur, json_file) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""
    try:
        cur.execute("""
            ALTER TABLE products
            ADD COLUMN IF NOT EXISTS supplier_id INT
        """)
        cur.execute("""
            ALTER TABLE products
            ADD CONSTRAINT fk_supplier_id
            FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
        """)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


if __name__ == '__main__':
    main()
