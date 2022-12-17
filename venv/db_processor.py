import sqlite3
import csv
import socket
import struct

# путь к БД
db_path = r'ip.db'
csv_path = 'ip_db.csv'
connection = None


# инициализация
def __init__():
    try:
        global connection
        connection = get_connection()
    except Exception as e:
        raise Exception("Ошибка инициализации программы: " + str(e))


# возвращает подключение
def get_connection():
    global connection
    try:
        connection = sqlite3.connect(db_path)
        return connection
    except Exception as e:
        raise Exception("Ошибка подключения к БД: " + str(e))


# инициализирует БД
def db_init():
    try:
        cur = connection.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS country(
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           code TEXT NOT NULL,
           name TEXT NOT NULL,
           CONSTRAINT constraint_country UNIQUE (code, name));
        """)
        cur.execute("""CREATE TABLE IF NOT EXISTS ip(
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           beg_ip INTEGER NOT NULL,
           end_ip INTEGER NOT NULL,
           country_id INTEGER NOT NULL,
           FOREIGN KEY(country_id) REFERENCES country(id),
           CONSTRAINT constraint_ip UNIQUE (beg_ip, end_ip));
        """)
        connection.commit()
    except Exception as e:
        raise Exception("Ошибка инициализации БД: " + str(e))


# возвращает id страны по коду
def get_country_id_by_code(country_code):
    try:
        cur = connection.cursor()
        cur.execute(f"""SELECT c.id FROM country c WHERE c.code = "{country_code}" LIMIT 1""")
        result = cur.fetchall()
        for row in result:
            return row[0]
        return None
    except Exception as e:
        raise Exception(f"Ошибка поиска идентификатора страны по коду {country_code}: " + str(e))


# получение страны по IP int
def get_country_by_ip(ip):
    try:
        cur = connection.cursor()
        cur.execute(f"""SELECT c.name 
        FROM country c, ip i 
        WHERE {ip} BETWEEN i.beg_ip AND i.end_ip 
            AND c.id = i.country_id
        LIMIT 1""")
        result = cur.fetchall()
        for row in result:
            return row[0]
        return None
    except Exception as e:
        raise Exception(f"Ошибка поиска идентификатора IP по {beg_ip}, {end_ip}: " + str(e))

# возвращает id страны по коду
def get_ip_id_by_beg_and_end(beg_ip, end_ip):
    try:
        cur = connection.cursor()
        cur.execute(f"""SELECT i.id FROM ip i WHERE i.beg_ip = "{beg_ip}" AND i.end_ip = "{end_ip}" LIMIT 1""")
        result = cur.fetchall()
        for row in result:
            return row[0]
        return None
    except Exception as e:
        raise Exception(f"Ошибка поиска идентификатора IP по {beg_ip}, {end_ip}: " + str(e))


# ковертирует ip в int
def ip2int(addr):
    return struct.unpack("!I", socket.inet_aton(addr))[0]


# ковертирует int в ip
def int2ip(addr):
    return socket.inet_ntoa(struct.pack("!I", addr))

# загрузка данных
def data_load():
    try:
        connection.commit()
        cur = connection.cursor()
        print(f"Импорт файла: {csv_path}")
        with open(csv_path) as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            row = 0
            errors = 0
            sql = None
            for i in reader:
                row += 1
                try:
                    if row % 100 == 0 or row == 1:
                        print(f"Обработка записи #{row}")
                        connection.commit()
                    country_id = get_country_id_by_code(i[2])
                    if country_id is None:
                        sql = f'''INSERT INTO country(code, name) VALUES("{i[2]}","{i[3]}") '''
                        cur.execute(sql)
                        country_id = cur.lastrowid
                    if get_ip_id_by_beg_and_end(i[0], i[1]) is None:
                        sql = f'''INSERT INTO ip(country_id, beg_ip, end_ip) VALUES({country_id}, {i[0]},{i[1]}) '''
                        cur.execute(sql)
                except Exception as e:
                    print(f"!Ошибка создания записи #{row}: {str(e)} sql: {sql}")
                    errors += 1
        print("Импорт завершен")
        print(f"Обработано записей: {row}")
        print(f"Ошибок обработки: {errors}")
    except Exception as e:
        raise Exception(f"Ошибка загрузки данных в БД: {str(e)}")


# основной блок
try:
    __init__()
    db_init()
    data_load()
    print(get_country_by_ip(ip2int("87.250.250.242")))
    print("ПРОГРАММА ЗАВЕРШЕНА")
except Exception as e:
    print("КРИТИЧЕСКАЯ ОШИБКА: " + str(e))
    raise SystemExit
finally:
    connection.close()
