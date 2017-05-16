
from decimal import Decimal
from pytz import UTC
from _mysql_exceptions import DataError, OperationalError, ProgrammingError
from unittest import TestCase

from my_mysql import MyMySQL
from my_utils import Fixture

class TestMySQLTableSelectGroupBy(TestCase):
    """
    https://dev.mysql.com/doc/refman/5.5/en/group-by-modifiers.html
    """
    def setUp(self):
        self.m = MyMySQL()
        self.f = Fixture('fixtures/select_group_by')

    def tearDown(self):
        self.m.ddl(self.f.drop_schema('select_group_by'))
        self.m = None
        self.f = None

    def test_01_single_column(self):
        inputs = 'single_column'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        self.assertEqual(long(expected_rows[0]), actual_rows[0][0])
        self.assertEqual(Decimal(expected_rows[1]), actual_rows[0][1])

    def test_02_multiple_columns(self):
        inputs = 'multiple_columns'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        for i in range(len(actual_rows)):
            self.assertEqual(long(expected_rows[i*3+0]), actual_rows[i][0][0])
            self.assertEqual(expected_rows[i*3+1], actual_rows[i][0][1])
            self.assertEqual(Decimal(expected_rows[i*3+2]), actual_rows[i][0][2])

    def test_03_roll_up(self):
        inputs = 'roll_up'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        for i in range(len(actual_rows)):
            if expected_rows[i*3+0] == 'None':
                self.assertEqual(None, actual_rows[i][0][0])
            else:
                self.assertEqual(long(expected_rows[i*3+0]),
                                 actual_rows[i][0][0])
            if expected_rows[i*3+1] == 'None':
                self.assertEqual(None, actual_rows[i][0][1])
            else:
                self.assertEqual(expected_rows[i*3+1],
                                 actual_rows[i][0][1])
            self.assertEqual(Decimal(expected_rows[i*3+2]), actual_rows[i][0][2])
