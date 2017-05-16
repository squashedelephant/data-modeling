
from _mysql_exceptions import DataError, OperationalError, ProgrammingError
from unittest import TestCase

from my_mysql import MyMySQL
from my_utils import Fixture

class TestMySQLTableUpdate(TestCase):
    """
    """
    def setUp(self):
        self.m = MyMySQL()
        self.f = Fixture('fixtures/update')

    def tearDown(self):
        self.m.ddl(self.f.drop_schema('update'))
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
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        for i in range(len(actual_rows)):
            self.assertEqual(long(expected_rows[i]), actual_rows[i][0][0])

    def test_03_all_rows(self):
        inputs = 'all_rows'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        for i in range(len(actual_rows)):
            self.assertEqual(expected_rows[i], actual_rows[i][0][0])
