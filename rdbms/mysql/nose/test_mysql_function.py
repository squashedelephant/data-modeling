
from datetime import date, datetime, timedelta
from decimal import Decimal
from pytz import UTC
from _mysql_exceptions import DataError, OperationalError, ProgrammingError
from unittest import TestCase
from uuid import UUID

from my_mysql import MyMySQL
from my_utils import Fixture

class TestMySQLFunction(TestCase):
    """
    https://dev.mysql.com/doc/refman/5.7/en/func-op-summary-ref.html
    """
    def setUp(self):
        self.m = MyMySQL()
        self.f = Fixture('fixtures/function')
        self.pst_in_secs = 7 * 60 * 60

    def tearDown(self):
        self.m.ddl(self.f.drop_schema('datatype'))
        self.m = None
        self.f = None
        self.pst_in_secs = 0

    def test_01_sum(self):
        inputs = 'sum'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = Decimal(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])

    def test_02_min(self):
        inputs = 'min'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = Decimal(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])

    def test_03_max(self):
        inputs = 'max'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = Decimal(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])

    def test_04_ceil(self):
        inputs = 'ceil'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = long(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])

    def test_05_floor(self):
        inputs = 'floor'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = long(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])

    def test_06_date(self):
        inputs = 'date'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        t = datetime.now()
        if t.hour > 16:
            t = t + timedelta(hours=8)
        actual = self.m.dql(self.f.test(inputs))
        expected = date(t.year, t.month, t.day)
        self.assertEqual(expected, actual[0][0])

    def test_07_time(self):
        inputs = 'time'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        t = datetime.now()
        expected = timedelta(hours=t.hour) +\
                   timedelta(minutes=t.minute) +\
                   timedelta(seconds=t.second) +\
                   timedelta(seconds=self.pst_in_secs)
        self.assertAlmostEqual(expected.seconds, actual[0][0].seconds, delta=3)

    def test_08_current_timestamp(self):
        inputs = 'current_timestamp'
        self.m.ddl(self.f.load_schema(inputs))
        actual = self.m.dql(self.f.test(inputs))
        t = datetime.now() + timedelta(seconds=self.pst_in_secs)
        expected = datetime(t.year, t.month, t.day, t.hour, t.minute, t.second)
        self.assertAlmostEqual(expected.second, actual[0][0].second, delta=3)

    def test_09_count(self):
        inputs = 'count'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = long(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])

    def test_10_date_format(self):
        inputs = 'date_format'
        self.m.ddl(self.f.load_schema(inputs))
        actual = self.m.dql(self.f.test(inputs))
        expected = datetime.now().strftime('%y %b %e')
        self.assertEqual(expected, actual[0][0])

    def test_11_if(self):
        inputs = 'if'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = self.f.expected(inputs)
        self.assertEqual(expected, actual[0][0])

    def test_12_in(self):
        inputs = 'in'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = long(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])

    def test_13_is_null(self):
        inputs = 'is_null'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = long(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])

    def test_14_is_not_null(self):
        inputs = 'is_not_null'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        self.assertEqual(long(expected_rows[0]), actual_rows[0][0][0])
        self.assertEqual(long(expected_rows[1]), actual_rows[1][0][0])

    def test_15_like(self):
        inputs = 'like'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = self.f.expected(inputs)
        self.assertEqual(expected, actual[0][0])

    def test_16_locate(self):
        inputs = 'locate'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = long(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])

    def test_17_md5(self):
        inputs = 'md5'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = self.f.expected(inputs)
        self.assertEqual(expected, actual[0][0])

    def test_18_mod(self):
        inputs = 'mod'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = long(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])

    def test_19_now(self):
        inputs = 'now'
        self.m.ddl(self.f.load_schema(inputs))
        actual = self.m.dql(self.f.test(inputs))
        t = datetime.now() + timedelta(seconds=self.pst_in_secs)
        expected = datetime(t.year, t.month, t.day, t.hour, t.minute, t.second)
        self.assertAlmostEqual(expected.second, actual[0][0].second, delta=3)

    def test_20_regexp(self):
        inputs = 'regex'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual_rows = self.m.dql(self.f.test(inputs))
        expected_rows = self.f.expected(inputs)
        self.assertEqual(expected_rows[0], actual_rows[0][0][0])
        self.assertEqual(expected_rows[1], actual_rows[1][0][0])
        self.assertEqual(expected_rows[2], actual_rows[2][0][0])
        
    def test_21_round(self):
        inputs = 'round'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.dml(self.f.populate(inputs))
        actual = self.m.dql(self.f.test(inputs))
        expected = Decimal(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])

    def test_22_rowcount(self):
        inputs = 'rowcount'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        actual = self.m.dql(self.f.test(inputs))
        expected = long(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])

    def test_23_substr(self):
        inputs = 'substr'
        self.m.ddl(self.f.load_schema(inputs))
        for stmt in self.f.populate(inputs):
            self.m.dml(stmt)
        actual = self.m.dql(self.f.test(inputs))
        expected = self.f.expected(inputs)
        self.assertEqual(expected, actual[0][0])

    def test_24_sysdate(self):
        inputs = 'sysdate'
        self.m.ddl(self.f.load_schema(inputs))
        actual = self.m.dql(self.f.test(inputs))
        t = datetime.now() + timedelta(seconds=self.pst_in_secs)
        expected = datetime(t.year, t.month, t.day, t.hour, t.minute, t.second)
        self.assertAlmostEqual(expected.second, actual[0][0].second, delta=3)

    def test_25_trim(self):
        inputs = 'trim'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        actual = self.m.dql(self.f.test(inputs))
        expected = self.f.expected(inputs)
        self.assertEqual(expected, actual[0][0])

    def test_26_user(self):
        inputs = 'user'
        self.m.ddl(self.f.load_schema(inputs))
        actual = self.m.dql(self.f.test(inputs))
        # hack around returned hostname
        expected = '{}@{}'.format(self.m.user, self.m.host[0:-2])
        self.assertEqual(expected, actual[0][0])

    def test_27_uuid(self):
        inputs = 'uuid'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        actual = self.m.dql(self.f.test(inputs))
        self.assertTrue(UUID(actual[0][0]))

    def test_28_sleep(self):
        inputs = 'sleep'
        self.m.ddl(self.f.load_schema(inputs))
        actual = self.m.dql(self.f.test(inputs))
        expected = long(self.f.expected(inputs))
        self.assertEqual(expected, actual[0][0])
