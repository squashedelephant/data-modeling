
from decimal import Decimal
from _mysql_exceptions import DataError, OperationalError, ProgrammingError
from unittest import TestCase

from my_mysql import MyMySQL
from my_utils import Fixture

class TestMySQLTableDelete(TestCase):
    """
    """
    def setUp(self):
        self.m = MyMySQL()
        self.f = Fixture('fixtures/delete')

    def tearDown(self):
        self.m.ddl(self.f.drop_schema('delete'))
        self.m = None
        self.f = None

    def test_01_single_row(self):
        inputs = 'single_row'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = long(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])

    def test_02_multiple_rows(self):
        inputs = 'multiple_rows'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = long(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])

    def test_03_all_rows(self):
        inputs = 'all_rows'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = long(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])

    def test_04_truncate(self):
        inputs = 'truncate'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = long(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])
