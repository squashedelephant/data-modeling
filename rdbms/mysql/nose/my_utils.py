from os.path import exists

class FixtureError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        self.msg

class Fixture:
    @classmethod
    def _load(cls, test_type, sql_type):
        dir = 'fixtures/datatype/'
        path = {'ddl': '{}/{}.ddl'.format(dir, test_type),
                'dml': '{}/{}.dml'.format(dir, test_type),
                'dql': '{}/{}.dql'.format(dir, test_type),
                'expected': '{}/{}.expected'.format(dir, test_type)}
        if not exists(path[sql_type]):
            return (False,
                    FixtureError('ERROR: cannot load {}'.format(path[sql_type])))
        try:
            f = open(path[sql_type], 'r')
            sql = []
            for line in f.readlines():
                sql.append(line.strip())
            f.close()
            if len(sql) == 1:
                return sql[0]
            else:
                return sql
        except IOError as e:
            return (False,
                    FixtureError('ERROR: cannot load {}'.format(path[sql_type])))

    @classmethod
    def load_schema(cls, test_type):
        return cls._load(test_type, 'ddl')

    @classmethod
    def populate(cls, test_type):
        return cls._load(test_type, 'dml')

    @classmethod
    def test(cls, test_type):
        return cls._load(test_type, 'dql')

    @classmethod
    def expected(cls, test_type):
        return cls._load(test_type, 'expected')
