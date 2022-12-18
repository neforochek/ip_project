import csv
import http.client
import socket
import sqlite3
import struct
import sys

from PyQt5.QtGui import QPixmap, QFont
from PyQt5.QtWidgets import QApplication, QLabel, QWidget, QPushButton, QMainWindow, QVBoxLayout, QLineEdit, \
    QPlainTextEdit

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


def get_country(ip):
    try:
        cur = connection.cursor()
        cur.execute(f"""SELECT c.code, c.name 
        FROM country c, ip i 
        WHERE {ip} BETWEEN i.beg_ip AND i.end_ip 
            AND c.id = i.country_id
        LIMIT 1""")
        result = cur.fetchall()
        for row in result:
            return row
        return None
    except Exception as e:
        raise Exception(f"Ошибка поиска идентификатора IP по {beg_ip}, {end_ip}: " + str(e))


# получает путь к файлу с изображение флага
def get_flag(country_code):
    return fr'flags/flags-medium/{country_code.lower()}.png'



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


    # db_init()
    # data_load()
    # print(get_country_by_ip(ip2int("46.72.11.190")))
    # print("ПРОГРАММА ЗАВЕРШЕНА")

    class IP(QWidget):
        def __init__(self):
            super().__init__()
            self.initUI()

        def set_flag(self, path):
            flag = QPixmap(path)
            flag = flag.scaledToWidth(100)
            self.image.setPixmap(flag)

        def initUI(self):
            self.setGeometry(0, 0, 500, 350)
            self.setMinimumSize(250, 270)
            self.setWindowTitle('Вычисление по IP')

            self.main_layout = QVBoxLayout(self)
            self.label_layout = QVBoxLayout(self)
            self.name_input_layout = QVBoxLayout(self)
            self.btn_my_ip_layout = QVBoxLayout(self)
            self.btn_look_inform_layout = QVBoxLayout(self)
            self.output_layout = QVBoxLayout(self)

            self.label = QLabel(self)
            self.label.setText("IP:")
            self.label.move(90, 50)
            self.label.resize(20, 10)

            self.name_input = QLineEdit(self)
            self.name_input.resize(250, 50)
            self.name_input.move(125, 30)

            self.btn_my_ip = QPushButton('Узнать свой IP', self)
            self.btn_my_ip.resize(150, 30)
            self.btn_my_ip.move(100, 90)
            self.btn_my_ip.clicked.connect(self.my_ip)

            self.btn_look_inform = QPushButton('Расчитать страну', self)
            self.btn_look_inform.resize(150, 30)
            self.btn_look_inform.move(250, 90)
            self.btn_look_inform.clicked.connect(self.country)

            self.output = QPlainTextEdit(self)
            self.output.resize(150, 30)
            self.output.move(200, 140)
            self.output.setEnabled(False)
            self.output.setStyleSheet("""
                font: bold 25px;
                color: black;
            """)

            self.label_layout.addWidget(self.label)
            self.name_input_layout.addWidget(self.name_input)
            self.btn_my_ip_layout.addWidget(self.btn_my_ip)
            self.btn_look_inform_layout.addWidget(self.btn_look_inform)
            self.output_layout.addWidget(self.output)

            self.main_layout.addLayout(self.label_layout)
            self.main_layout.addLayout(self.name_input_layout)
            self.main_layout.addLayout(self.btn_my_ip_layout)
            self.main_layout.addLayout(self.btn_look_inform_layout)
            self.main_layout.addLayout(self.output_layout)

            self.pixmap = QPixmap(r"D:\dev\ip_project\venv\flags\flags-medium\ip_start.png")
            self.pixmap = self.pixmap.scaledToWidth(100)
            self.image = QLabel(self)
            self.image.resize(self.pixmap.width(), self.pixmap.height())
            self.image.move(15, 160)
            self.image.setPixmap(self.pixmap)
            self.setLayout(self.main_layout)


        def my_ip(self):
            self.conn = http.client.HTTPConnection("ifconfig.me")
            self.conn.request("GET", "/ip")
            self.name_input.setText(self.conn.getresponse().read().decode('UTF-8'))

        def country(self):
            try:
                enter_ip = self.name_input.text()
                if len(get_country_by_ip(ip2int(enter_ip))) > 0:
                    country = get_country(ip2int(enter_ip))
                    self.output.setPlainText(country[1])
                    self.set_flag(get_flag(country[0]))
                else:
                    self.output.setPlainText('Неверный ввод.')
            except:
                if enter_ip == '':
                    self.output.setPlainText('Введите IP.')
                else:
                    self.output.setPlainText('Неверный ввод.')
                self.set_flag(get_flag('error'))

    if __name__ == '__main__':
        app = QApplication(sys.argv)

        # window = MainWindow()
        # window.show()

        ex = IP()
        ex.show()


        sys.exit(app.exec())

except Exception as e:
    print("КРИТИЧЕСКАЯ ОШИБКА: " + str(e))
    raise SystemExit
finally:
    connection.close()
