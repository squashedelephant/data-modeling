
from datetime import date, datetime, timedelta
from decimal import Decimal
from pytz import UTC
from _mysql_exceptions import DataError, OperationalError, ProgrammingError
from unittest import TestCase

from my_mysql import MyMySQL
from my_utils import Fixture

class TestMySQLTableSelect(TestCase):
    """
    https://dev.mysql.com/doc/refman/5.7/en/func-op-summary-ref.html
    """
    def setUp(self):
        self.m = MyMySQL()
        self.f = Fixture('fixtures/select')
        self.today = date.today()
        self.times = []
        self.pdt_in_secs = 7 * 60 * 60

    def tearDown(self):
        for stmt in self.f.drop_schema('select'):
            self.m.ddl(stmt)
        self.m = None
        self.f = None
        self.today = None
        self.times = None

    def test_01_all_columns(self):
        inputs = 'all_columns'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
            t = datetime.now()
            self.times.append(timedelta(hours=t.hour) +\
                              timedelta(minutes=t.minute) +\
                              timedelta(seconds=t.second) +\
                              timedelta(seconds=self.pdt_in_secs))
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        for i in range(len(actual_rows)):
            self.assertEqual(long(expected_rows[i*6+0]), actual_rows[i][0][0])
            self.assertEqual(long(expected_rows[i*6+1]), actual_rows[i][0][1])
            self.assertEqual(long(expected_rows[i*6+2]), actual_rows[i][0][2])
            self.assertEqual(self.today, actual_rows[i][0][3])
            self.assertAlmostEqual(self.times[i].seconds,
                                   actual_rows[i][0][4].seconds, delta=3)
            self.assertEqual(Decimal(expected_rows[i*6+5]), actual_rows[i][0][5])

    def test_02_single_column(self):
        inputs = 'single_column'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual_row = self.m.dql(self.f.test(inputs))
        expected_row = self.f.expected(inputs)
        self.assertEqual(Decimal(expected_row), actual_row[0][0][0])

    def test_03_multiple_columns(self):
        inputs = 'multiple_columns'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        self.assertEqual(Decimal(expected_rows[0]), actual_rows[0][0][0])
        self.assertEqual(Decimal(expected_rows[1]), actual_rows[0][0][1])

    def test_04_where_column_equals_value(self):
        inputs = 'where_column_equals_value'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual_row = self.m.dql(self.f.test(inputs))
        expected_row = self.f.expected(inputs)
        self.assertEqual(Decimal(expected_row), actual_row[0][0])

    def test_05_where_column_in(self):
        inputs = 'where_column_in'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual_row = self.m.dql(self.f.test(inputs))
        expected_row = self.f.expected(inputs)
        self.assertEqual(expected_row, actual_row[0][0])

    def test_06_where_column_between(self):
        inputs = 'where_column_between'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual_row = self.m.dql(self.f.test(inputs))
        expected_row = self.f.expected(inputs)
        self.assertEqual(Decimal(expected_row), actual_row[0][0])

    def test_07_where_column_unequal_to_value(self):
        inputs = 'where_column_unequal_to_value'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual_row = self.m.dql(self.f.test(inputs))
        expected_row = self.f.expected(inputs)
        self.assertEqual(Decimal(expected_row), actual_row[0][0])

    def test_08_where_column_not_value(self):
        inputs = 'where_column_not_value'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual_row = self.m.dql(self.f.test(inputs))
        expected_row = self.f.expected(inputs)
        self.assertEqual(Decimal(expected_row), actual_row[0][0])

    def test_09_multiple_columns_multiple_tables(self):
        inputs = 'multiple_columns_multiple_tables'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        self.assertEqual(expected_rows[0], actual_rows[0][0])
        self.assertEqual(expected_rows[1], actual_rows[0][1])
        self.assertEqual(expected_rows[2], actual_rows[0][2])
        self.assertEqual(Decimal(expected_rows[3]), actual_rows[0][3])
