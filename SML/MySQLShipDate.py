import pymysql


class ship_date_database:
    # 初始化异常处理字典
    def __init__(self):
        self.except_dict = {0: None, 1: None, 2: None, 3: None, 4: None}

    # 连接数据库
    def connect_database(self, host, user, password, database):
        try:
            self.db = pymysql.connect(host=host, user=user, password=password, database=database)
            self.cursor = self.db.cursor()
        except Exception as e:
            self.except_dict[0] = e
        else:
            self.except_dict[0] = 'OK'

    # 获取carrier表
    def get_carrier(self):
        try:
            self.cursor.execute("SELECT `id`,`nameEn` FROM carrier")
            carrier_data = self.cursor.fetchall()
        except Exception as e:
            self.except_dict[1] = e
        else:
            carrier_dict = {}
            for data_tuple in carrier_data:
                if data_tuple[1]:
                    carrier_dict[data_tuple[1].upper()] = data_tuple[0]
            self.except_dict[1] = 'OK'
            return carrier_dict

    # 获取port表,英文名映射
    def get_port(self):
        try:
            self.cursor.execute("SELECT `id`,`nameEn` FROM port")
            port_data = self.cursor.fetchall()
        except Exception as e:
            self.except_dict[2] = e
        else:
            port_dict = {}
            for data_tuple in port_data:
                if data_tuple[1]:
                    port_dict[data_tuple[1].upper()] = data_tuple[0]
            self.except_dict[2] = 'OK'
            return port_dict

    # 获取port表,中文名映射
    def get_port_chinese(self):
        try:
            self.cursor.execute("SELECT `id`,`name` FROM port")
            port_chinese_data = self.cursor.fetchall()
        except Exception as e:
            self.except_dict[3] = e
        else:
            port_chinese_dict = {}
            for data_tuple in port_chinese_data:
                if data_tuple[1]:
                    port_chinese_dict[data_tuple[1].upper()] = data_tuple[0]
            self.except_dict[3] = 'OK'
            return port_chinese_dict

    # 获取dictionary表
    def get_dictionary(self):
        try:
            self.cursor.execute("SELECT `name`,`alias` FROM dictionary")
            dictionary_data = self.cursor.fetchall()
        except Exception as e:
            self.except_dict[4] = e
        else:
            dictionary_dict = {}
            for data_tuple in dictionary_data:
                if data_tuple[0] and data_tuple[1]:
                    dictionary_dict[data_tuple[1].upper()] = data_tuple[0]
            self.except_dict[4] = 'OK'
            return dictionary_dict

    # 关闭数据库
    def close_database(self):
        try:
            self.db.close()
        except:
            pass


database = ship_date_database()
database.connect_database("localhost", "root", "123456", "api_server")
carrier = database.get_carrier()
port = database.get_port()
port_chinese = database.get_port_chinese()
dictionary = database.get_dictionary()
except_dict = database.except_dict
database.close_database()

if __name__ == '__main__':
    database = ship_date_database()
    database.connect_database("localhost", "root", "123456", "api_server")
    carrier = database.get_carrier()
    port = database.get_port()
    port_chinese = database.get_port_chinese()
    dictionary = database.get_dictionary()
    except_dict = database.except_dict
    database.close_database()
    print(except_dict)
