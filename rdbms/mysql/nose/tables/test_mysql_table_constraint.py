
from _mysql_exceptions import DataError, OperationalError, ProgrammingError
from decimal import Decimal
from unittest import TestCase

from my_mysql import MyMySQL
from my_utils import Fixture

class TestMySQLTableConstraint(TestCase):
    """
    https://dev.mysql.com/doc/refman/5.7/en/constraints.html
    """
    def setUp(self):
        self.m = MyMySQL()
        self.f = Fixture('fixtures/constraint')

    def tearDown(self):
        for stmt in self.f.drop_schema('constraint'):
            self.m.ddl(stmt)
        self.m = None
        self.f = None

    def test_01_valid_data(self):
        inputs = 'valid_data'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        for i in range(len(actual_rows)):
            if expected_rows[i*2+0] == 'None':
                self.assertEqual('', actual_rows[i][0][0])
            else:
                self.assertEqual(expected_rows[i*2+0], actual_rows[i][0][0])
            self.assertEqual(expected_rows[i*2+1], actual_rows[i][0][1])

    def test_02_invalid_data(self):
        inputs = 'invalid_data'
        self.m.ddl(self.f.load_schema(inputs))
        with self.assertRaises(OperationalError) as context:
            self.m.dml(self.f.populate(inputs))
        self.assertIn('doesn\'t have a default value', str(context.exception))

    def test_03_valid_fk(self):
        inputs = 'valid_fk'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        self.assertEqual(expected_rows[0], actual_rows[0][0])
        self.assertEqual(long(expected_rows[1]), actual_rows[0][1])
        self.assertEqual(Decimal(expected_rows[2]), actual_rows[0][2])

    def test_04_invalid_fk(self):
        inputs = 'invalid_fk'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        with self.assertRaises(OperationalError) as context:
            self.m.dml(self.f.populate(inputs))
        self.assertIn('cannot be null', str(context.exception))
