from os.path import exists
from sys import exit

class Fixture:
    def __init__(self, dir):
        self.dir = dir

    def _load(self, test_type, sql_type):
        path = {'ddl': '{}/{}.ddl'.format(self.dir, test_type),
                'dml': '{}/{}.dml'.format(self.dir, test_type),
                'dql': '{}/{}.dql'.format(self.dir, test_type),
                'expected': '{}/{}.expected'.format(self.dir, test_type),
                'clean': '{}/drop_schema.ddl'.format(self.dir)}
        if not exists(path[sql_type]):
            exit('ERROR: cannot load {}'.format(path[sql_type]))
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

    def load_schema(self, test_type):
        return self._load(test_type, 'ddl')

    def populate(self, test_type):
        return self._load(test_type, 'dml')

    def test(self, test_type):
        return self._load(test_type, 'dql')

    def expected(self, test_type):
        return self._load(test_type, 'expected')

    def drop_schema(self, test_type):
        return self._load(test_type, 'clean')

