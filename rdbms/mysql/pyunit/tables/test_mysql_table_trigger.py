
from _mysql_exceptions import DataError, OperationalError, ProgrammingError
from decimal import Decimal
from unittest import TestCase

from my_mysql import MyMySQL
from my_utils import Fixture

class TestMySQLTableTrigger(TestCase):
    """
    https://dev.mysql.com/doc/refman/5.7/en/trigger-syntax.html
    """
    def setUp(self):
        self.m = MyMySQL()
        self.f = Fixture('fixtures/trigger')

    def tearDown(self):
        self.m.ddl(self.f.drop_schema('trigger'))
        self.m = None
        self.f = None

    def test_01_before_insert(self):
        inputs = 'before_insert'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = Decimal(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])

    def test_02_after_insert(self):
        inputs = 'after_insert'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = Decimal(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])

    def test_03_before_update(self):
        inputs = 'before_update'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = Decimal(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])

    def test_04_after_update(self):
        inputs = 'after_update'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = Decimal(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])

    def test_05_before_delete(self):
        inputs = 'before_delete'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = long(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])

    def test_06_after_delete(self):
        inputs = 'after_delete'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = long(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])
