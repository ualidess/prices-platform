import os
import re
import pandas as pd
import psycopg2

# Настрой подключение к PostgreSQL
conn = psycopg2.connect(
    dbname="prices_db",
    user="postgres",
    password="Raidraid27_", 
    host="localhost",
    port="5432"
)
conn.autocommit = True
cur = conn.cursor()

def get_id(table, name_field, name_value):
    cur.execute(f"SELECT id FROM {table} WHERE {name_field} = %s", (name_value,))
    row = cur.fetchone()
    if row:
        return row[0]
    if table == 'cities':
        cur.execute("INSERT INTO cities (name) VALUES (%s) RETURNING id", (name_value,))
        return cur.fetchone()[0]
    elif table == 'products':
        cur.execute("INSERT INTO products (name, unit) VALUES (%s, %s) RETURNING id",
                    (name_value, 'тенге/кг'))
        return cur.fetchone()[0]
    raise ValueError("Unknown table")

def parse_excel(file_path, year, month):
    # читаем Excel, пропуская первые строки-шапки
    df = pd.read_excel(file_path, header=2)
    df = df.dropna(how='all')
    city_names = [str(c).strip() for c in df.columns[1:]]

    for _, row in df.iterrows():
        product_name = str(row.iloc[0]).strip()
        if not product_name or product_name.lower().startswith('расчеты'):
            continue
        for idx, city in enumerate(city_names, start=1):
            price = row.iloc[idx]
            if pd.isna(price):
                continue

            # безопасное преобразование цены
            try:
                price_value = float(str(price).replace(',', '.').strip())
            except ValueError:
                # если не удалось преобразовать в число — пропускаем
                continue

            product_id = get_id('products', 'name', product_name)
            city_id = get_id('cities', 'name', city)
            cur.execute("""
                INSERT INTO product_prices (product_id, city_id, year, month, price, source)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id, city_id, year, month)
                DO UPDATE SET price = EXCLUDED.price, source = EXCLUDED.source
            """, (product_id, city_id, year, month, price_value, f"{year}-{month:02d}"))

def infer_year_month(filename):
    m = re.search(r'(\d{4})_(\d{2})', filename)
    if m:
        return int(m.group(1)), int(m.group(2))
    raise ValueError(f"Не удалось определить год/месяц из имени: {filename}")

def main():
    raw_dir = os.path.join('data', 'raw')
    for fname in os.listdir(raw_dir):
        if fname.lower().endswith(('.xls', '.xlsx')):
            year, month = infer_year_month(fname)
            print(f"Импорт: {fname} ({year}-{month:02d})")
            parse_excel(os.path.join(raw_dir, fname), year, month)
    print("Импорт завершён.")

if __name__ == "__main__":
    main()
