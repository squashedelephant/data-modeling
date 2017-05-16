
from decimal import Decimal
from pytz import UTC
from _mysql_exceptions import DataError, OperationalError, ProgrammingError
from unittest import TestCase

from my_mysql import MyMySQL
from my_utils import Fixture

class TestMySQLTableSelectOrderBy(TestCase):
    """
    https://www.w3schools.com/sql/sql_orderby.asp
    """
    def setUp(self):
        self.m = MyMySQL()
        self.f = Fixture('fixtures/select_order_by')

    def tearDown(self):
        for stmt in self.f.drop_schema('select_order_by'):
            self.m.ddl(stmt)
        self.m = None
        self.f = None

    def test_01_reverse_order(self):
        inputs = 'reverse_order'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.dml(self.f.populate(inputs))
        actual = self.m.dql(self.f.test(inputs))
        expected = self.f.expected(inputs)
        self.assertEqual(expected[0], actual[0][0][0])
        self.assertEqual(expected[1], actual[1][0][0])
        self.assertEqual(expected[2], actual[2][0][0])
        self.assertEqual(expected[3], actual[3][0][0])
        self.assertEqual(expected[4], actual[4][0][0])

    def test_02_multiple_columns_asc(self):
        inputs = 'multiple_columns_asc'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.dml(self.f.populate(inputs))
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        for i in range(len(actual_rows)):
            self.assertEqual(long(expected_rows[i*3+0]), actual_rows[i][0][0])
            self.assertEqual(expected_rows[i*3+1], actual_rows[i][0][1])
            self.assertEqual(expected_rows[i*3+2], actual_rows[i][0][2])

    def test_03_multiple_columns_asc_desc(self):
        inputs = 'multiple_columns_asc_desc'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.dml(self.f.populate(inputs))
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        for i in range(len(actual_rows)):
            self.assertEqual(long(expected_rows[i*3+0]), actual_rows[i][0][0])
            self.assertEqual(expected_rows[i*3+1], actual_rows[i][0][1])
            self.assertEqual(expected_rows[i*3+2], actual_rows[i][0][2])
