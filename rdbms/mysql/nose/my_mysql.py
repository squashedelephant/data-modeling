from os import environ
from os.path import exists

from MySQLdb import connect, DataError, OperationalError, ProgrammingError

class MyMySQLError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

class MyMySQL(object):
    def __init__(self):
        self.path = environ.get('HOME') + '/.ssh/' + 'my.cnf'
        self.config = self._load_config()
        self.host = self.config['host']
        self.port = int(self.config['port'])
        self.user = self.config['user']
        self.password = self.config['password']
        self.charset = self.config['default-character-set']
        self.name = self.config['database']
        self.db = None
        try:
            self.db = connect(host=self.host,
                              port=self.port,
                              user=self.user,
                              passwd=self.password,
                              db=self.name)
        except ProgrammingError, e:
            exit('ERROR: connection to {}:{} lost!'.format(self.host, self.port))

    def _load_config(self):
        if not exists(self.path):
            exit('ERROR: invalid path to my.cnf: {}'.format(self.path))
        config = dict()
        try:
            self.f = open(self.path, 'r')
            for line in self.f.readlines():
                if line.startswith('#') or \
                   line.startswith('[') or \
                   len(line) < 2:
                    continue
                (key, value) = line.strip().split('=')
                config[key.strip()] = value.strip()
            self.f.close()
            return config
        except IOError as e:
            exit('ERROR: unable to read MySQL config: {}'.format(self.path))

    def _get_row_key_equals_value(self, db, table, column, key, value):
        try:
            sql = 'SELECT ' + column + ' FROM ' + table + ' WHERE ' + key +\
                '="' + value + '";'
            self.db.query(sql)
            return self.db.store_result().fetch_row(maxrows=1)[0][0]
        except IndexError, e:
            raise MyMySQLError('IndexError: {}'.format(str(e)))
        except OperationalError, e:
            raise MyMySQLError('OperationalError: {}'.format(str(e)))
        except ProgrammingError, e:
            raise MyMySQLError('ProgrammingError: {}'.format(str(e)))

    def _exec(self, sql, cmd):
        self.db.query(sql)
        records = list()
        if cmd in ['ddl', 'dml']:
            return
        elif cmd == 'dql':
            result = self.db.store_result()
            rows = result.num_rows()
            if rows > 1:
                while True:
                    record = result.fetch_row()
                    if not record:
                        break
                    records.append(record)
                return records
            else:
                return result.fetch_row(maxrows=1)[0][0]

    def ddl(self, stmt):
        return self._exec(stmt, 'ddl')

    def dml(self, stmt):
        return self._exec(stmt, 'dml')

    def dql(self, query):
        return self._exec(query, 'dql')

    def get_tables(self):
        stmt = 'SHOW TABLES;'
        return self._exec(stmt, 'ddl')

    def get_count(self, table):
        query = 'SELECT COUNT(*) FROM ' + table + ';'
        return self._exec(query, 'dql')

