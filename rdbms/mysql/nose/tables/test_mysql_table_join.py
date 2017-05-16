
from datetime import date, datetime, timedelta
from decimal import Decimal
from pytz import UTC
from _mysql_exceptions import DataError, OperationalError, ProgrammingError
from unittest import TestCase

from my_mysql import MyMySQL
from my_utils import Fixture

class TestMySQLTableJoin(TestCase):
    """
    """
    def setUp(self):
        self.m = MyMySQL()
        self.f = Fixture('fixtures/join')
        self.stmt = None
        self.query = None

    def tearDown(self):
        for stmt in self.f.drop_schema('join'):
            self.m.ddl(stmt)
        self.m = None
        self.f = None

    def test_01_inner_join(self):
        inputs = 'inner_join'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        for i in range(len(actual_rows)):
            self.assertEqual(expected_rows[i*3+0], actual_rows[i][0][0])
            self.assertEqual(expected_rows[i*3+1], actual_rows[i][0][1])
            self.assertEqual(expected_rows[i*3+2], actual_rows[i][0][2])

    def test_02_left_join(self):
        inputs = 'left_join'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        today = date.today()
        tomorrow = date.today() + timedelta(days=1)
        self.assertEqual(expected_rows[0], actual_rows[0][0][0])
        self.assertEqual(long(expected_rows[1]), actual_rows[0][0][1])
        self.assertEqual(today, actual_rows[0][0][2])
        self.assertEqual(expected_rows[3], actual_rows[1][0][0])
        self.assertEqual(long(expected_rows[4]), actual_rows[1][0][1])
        self.assertEqual(tomorrow, actual_rows[1][0][2])
        self.assertEqual(expected_rows[6], actual_rows[2][0][0])
        self.assertEqual(None, actual_rows[2][0][1]);
        self.assertEqual(None, actual_rows[2][0][2]);

    def test_03_right_join(self):
        inputs = 'right_join'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        today = date.today()
        tomorrow = date.today() + timedelta(days=1)
        self.assertEqual(expected_rows[0], actual_rows[0][0][0])
        self.assertEqual(long(expected_rows[1]), actual_rows[0][0][1])
        self.assertEqual(today, actual_rows[0][0][2])
        self.assertEqual(expected_rows[3], actual_rows[1][0][0])
        self.assertEqual(long(expected_rows[4]), actual_rows[1][0][1])
        self.assertEqual(tomorrow, actual_rows[1][0][2])
        self.assertEqual(expected_rows[6], actual_rows[2][0][0])
        self.assertEqual(None, actual_rows[2][0][1]);
        self.assertEqual(None, actual_rows[2][0][2]);

    def test_04_full_outer_join(self):
        # technically, this is a UNION of left join with right join
        inputs = 'full_join'
        for stmt in self.f.load_schema(inputs):
            self.m.ddl(stmt)
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        today = date.today()
        tomorrow = date.today() + timedelta(days=1)
        self.assertEqual(expected_rows[0], actual_rows[0][0][0])
        self.assertEqual(long(expected_rows[1]), actual_rows[0][0][1])
        self.assertEqual(today, actual_rows[0][0][2])
        self.assertEqual(expected_rows[3], actual_rows[1][0][0])
        self.assertEqual(long(expected_rows[4]), actual_rows[1][0][1])
        self.assertEqual(tomorrow, actual_rows[1][0][2])
        self.assertEqual(expected_rows[6], actual_rows[2][0][0])
        self.assertEqual(None, actual_rows[2][0][1]);
        self.assertEqual(None, actual_rows[2][0][2]);

    def ttest_05_self_join(self):
        pass

