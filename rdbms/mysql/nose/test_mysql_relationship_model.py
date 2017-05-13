from datetime import datetime, timedelta
from pytz import UTC
from _mysql_exceptions import DataError, OperationalError, ProgrammingError
from unittest import TestCase

from my_mysql import MyMySQL
from my_utils import Fixture

class TestMySQLRelationshipModel(TestCase):
    """
    """
    def setUp(self):
        self.m = MyMySQL()
        self.f = Fixture('fixtures/relationship')

    def tearDown(self):
        for stmt in self.f.drop_schema('datatype'):
            self.m.ddl(stmt)
        self.m = None
        self.f = None

    def test_01_foreign_key(self):
        inputs = 'foreign_key'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.ddl(stmt)
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        for i in range(len(actual_rows)):
            self.assertEqual(expected_rows[i*4+0], actual_rows[i][0][0])
            self.assertEqual(expected_rows[i*4+1], actual_rows[i][0][1])
            self.assertEqual(expected_rows[i*4+2], actual_rows[i][0][2])
            self.assertEqual(expected_rows[i*4+3], actual_rows[i][0][3])

    def test_02_many_to_many(self):
        inputs = 'many_to_many'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.ddl(stmt)
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        for i in range(len(actual_rows)):
            self.assertEqual(expected_rows[i*3+0], actual_rows[i][0][0])
            self.assertEqual(expected_rows[i*3+1], actual_rows[i][0][1])
            self.assertEqual(expected_rows[i*3+2], actual_rows[i][0][2])

    def test_03_one_to_many(self):
        inputs = 'one_to_many'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.ddl(stmt)
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        for i in range(len(actual_rows)):
            self.assertEqual(expected_rows[i*2+0], actual_rows[i][0][0])
            self.assertEqual(expected_rows[i*2+1], actual_rows[i][0][1])

    def test_04_one_to_one(self):
        inputs = 'one_to_one'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.ddl(stmt)
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        for i in range(len(actual_rows)):
            self.assertEqual(expected_rows[i*3+0], actual_rows[i][0][0])
            self.assertEqual(expected_rows[i*3+1], actual_rows[i][0][1])
            self.assertEqual(long(expected_rows[i*3+2]), actual_rows[i][0][2])
