import json
import pymssql


class MSSQLConnection:
    def __init__(self, settings_filename='settings.json'):
        self.__connected = False
        with open(settings_filename, 'r', encoding='utf-8') as f:
            settings = json.loads(f.read())

        self.__server = settings['server']
        self.__database = settings['database']
        self.__login = settings['login']
        self.__password = settings['password']
        self.__driver = "{SQL Server Native Client 11.0}"

        try:
            self.__connection = pymssql.connect(self.__server, self.__login, self.__password, self.__database)
            self.__connected = True
        except Exception as E:
            print(f"Exception while creating connection class: {E}")
            self.__connected = False
            self.__connection = None

    @property
    def connection(self):
        return self.__connection

    @property
    def connected(self):
        return self.__connected

    def execute(self, query, params=()):
        if self.connection is not None:
            try:
                if len(params) > 0:
                    self.connection.cursor().execute(query, params)
                    self.connection.commit()
                else:
                    self.connection.cursor().execute(query)
                    self.connection.commit()
                return True
            except Exception as E:
                print(f"Exception while execute: {E}")
                return False
        else:
            return False

    def select(self, query, params=()):
        if self.connection is not None:
            try:
                cursor = self.connection.cursor()
                if len(params) > 0:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                for row in cursor:
                    yield [row[el] for el in range(0, len(row))]
                cursor.close()
            except Exception as E:
                print(f'Exception while select: {E}')
                return ()
