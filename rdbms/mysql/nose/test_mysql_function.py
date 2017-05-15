
from datetime import date, datetime, timedelta
from decimal import Decimal
from pytz import UTC
from _mysql_exceptions import DataError, OperationalError, ProgrammingError
from unittest import TestCase

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
        if t.hour > 16:
            t = t + timedelta(hours=8)
        expected = timedelta(hours=t.hour) +\
                   timedelta(minutes=t.minute) +\
                   timedelta(seconds=t.second) +\
                   timedelta(seconds=self.pst_in_secs)
        self.assertEqual(expected, actual[0][0])

    def ttest_08_current_timestamp(self):
        self.stmt = 'CREATE TABLE data(id MEDIUMINT);'
        self.m.ddl(self.stmt)
        self.stmt = 'INSERT INTO data(id) VALUES(-8388608);'
        self.m.dml(self.stmt)
        self.query = 'SELECT id FROM data;'
        self.assertEqual('-8388608', self.m.dql(self.query))

    def ttest_09_count(self):
        self.stmt = 'CREATE TABLE data(id MEDIUMINT);'
        self.m.ddl(self.stmt)
        self.stmt = 'INSERT INTO data(id) VALUES(8388607);'
        self.m.dml(self.stmt)
        self.query = 'SELECT id FROM data;'
        self.assertEqual('8388607', self.m.dql(self.query))

    def ttest_10_format(self):
        self.stmt = 'CREATE TABLE data(id INT);'
        self.m.ddl(self.stmt)
        self.stmt = 'INSERT INTO data(id) VALUES(-2147483648);'
        self.m.dml(self.stmt)
        self.query = 'SELECT id FROM data;'
        self.assertEqual('-2147483648', self.m.dql(self.query))

    def ttest_11_if(self):
        self.stmt = 'CREATE TABLE data(id INT);'
        self.m.ddl(self.stmt)
        self.stmt = 'INSERT INTO data(id) VALUES(2147483647);'
        self.m.dml(self.stmt)
        self.query = 'SELECT id FROM data;'
        self.assertEqual('2147483647', self.m.dql(self.query))

    def ttest_12_in(self):
        self.stmt = 'CREATE TABLE data(id BIGINT);'
        self.m.ddl(self.stmt)
        self.stmt = 'INSERT INTO data(id) VALUES(-9223372036854775808);'
        self.m.dml(self.stmt)
        self.query = 'SELECT id FROM data;'
        self.assertEqual('-9223372036854775808', self.m.dql(self.query))

    def ttest_13_is_null(self):
        self.stmt = 'CREATE TABLE data(id BIGINT);'
        self.m.ddl(self.stmt)
        self.stmt = 'INSERT INTO data(id) VALUES(9223372036854775807);'
        self.m.dml(self.stmt)
        self.query = 'SELECT id FROM data;'
        self.assertEqual('9223372036854775807', self.m.dql(self.query))

    def ttest_14_is_not_null(self):
        self.stmt = 'CREATE TABLE data(money DECIMAL(5,2));'
        self.m.ddl(self.stmt)
        self.stmt = 'INSERT INTO data(money) VALUES(100.3333);'
        self.m.dml(self.stmt)
        self.query = 'SELECT money FROM data;'
        self.assertEqual('100.33', self.m.dql(self.query))

    def ttest_15_like(self):
        self.stmt = 'CREATE TABLE data(money DECIMAL(5,2));'
        self.m.ddl(self.stmt)
        self.stmt = 'INSERT INTO data(money) VALUES(100.499);'
        self.m.dml(self.stmt)
        self.query = 'SELECT money FROM data;'
        self.assertEqual('100.50', self.m.dql(self.query))

    def ttest_16_locate(self):
        self.stmt = 'CREATE TABLE data(money DECIMAL(5,2));'
        self.m.ddl(self.stmt)
        self.stmt = 'INSERT INTO data(money) VALUES(100.0009);'
        self.m.dml(self.stmt)
        self.query = 'SELECT money FROM data;'
        self.assertEqual('100.00', self.m.dql(self.query))

    def ttest_17_md5(self):
        self.stmt = 'CREATE TABLE data(percent FLOAT(5,2));'
        self.m.ddl(self.stmt)
        self.stmt = 'INSERT INTO data(percent) VALUES(100.0000);'
        self.m.dml(self.stmt)
        self.query = 'SELECT percent FROM data;'
        self.assertEqual('100.00', self.m.dql(self.query))

    def ttest_17_mod(self):
        self.stmt = 'CREATE TABLE data(ppm FLOAT(6,3));'
        self.m.ddl(self.stmt)
        self.stmt = 'INSERT INTO data(ppm) VALUES(100.106);'
        self.m.dml(self.stmt)
        self.query = 'SELECT ppm FROM data;'
        self.assertEqual('100.106', self.m.dql(self.query))

    def ttest_18_now(self):
        # https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_current-timestamp
        # https://docs.python.org/2/library/datetime.html
        self.stmt = 'CREATE TABLE data(created CHAR(20));'
        self.m.ddl(self.stmt)
        self.stmt = "INSERT INTO data(created) VALUES (DATE_FORMAT(NOW(), '%b %d %Y %h:%i %p'));"
        self.m.dml(self.stmt)
        self.query = 'SELECT created FROM data;'
        expected = datetime.now().strftime('%b %d %Y %H:%M %p')
        self.assertEqual(expected, self.m.dql(self.query))

    def ttest_19_regexp(self):
        self.stmt = 'CREATE TABLE data(created CHAR(19));'
        self.m.ddl(self.stmt)
        self.stmt = 'INSERT INTO data(created) VALUES (CURRENT_DATE());'
        self.m.dml(self.stmt)
        self.query = 'SELECT created FROM data;'
        t = datetime.now()
        print(t.strftime('%Y-%m-%d %H:%M:%S'))
        print('hour: {}'.format(t.hour))
        if t.hour > 1600:
            expected = (datetime(t.year, t.month, t.day) +\
                        timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            expected = datetime(t.year, t.month, t.day).strftime('%Y-%m-%d')
        self.assertEqual(expected, self.m.dql(self.query))

    def ttest_20_round(self):
        self.stmt = 'CREATE TABLE data(created CHAR(19));'
        self.m.ddl(self.stmt)
        pass

    def ttest_20_rowcount(self):
        self.stmt = 'CREATE TABLE data(created CHAR(19));'
        self.m.ddl(self.stmt)
        pass

    def ttest_21_substr(self):
        self.stmt = 'CREATE TABLE data(created CHAR(19));'
        self.m.ddl(self.stmt)
        pass

    def ttest_22_sysdate(self):
        self.stmt = 'CREATE TABLE data(created CHAR(19));'
        self.m.ddl(self.stmt)
        pass

    def ttest_23_trim(self):
        name = 'a'
        self.stmt = 'CREATE TABLE data(name CHAR(1));'
        self.m.ddl(self.stmt)
        self.stmt = "INSERT INTO data(name) VALUES('{}');".format(name)
        self.m.dml(self.stmt)
        self.query = 'SELECT name FROM data;'
        self.assertEqual(name, self.m.dql(self.query))

    def ttest_23_upper(self):
        name = 'Wonder Woman'
        self.stmt = 'CREATE TABLE data(name CHAR(15));'
        self.m.ddl(self.stmt)
        self.stmt = "INSERT INTO data(name) VALUES('{}');".format(name)
        self.m.dml(self.stmt)
        self.query = 'SELECT name FROM data;'
        self.assertEqual(name, self.m.dql(self.query))


    def ttest_24_user(self):
        trailing_spaces = '    '
        name = 'Wonder Woman' + trailing_spaces
        self.stmt = 'CREATE TABLE data(name CHAR(15));'
        self.m.ddl(self.stmt)
        self.stmt = "INSERT INTO data(name) VALUES('{}');".format(name)
        self.m.dml(self.stmt)
        self.query = 'SELECT name FROM data;'
        self.assertEqual(name, self.m.dql(self.query) + trailing_spaces)

    def ttest_25_uuid(self):
        phrase = 'The quick brown fox jumped over the lazy dog.'
        self.stmt = 'CREATE TABLE data(name VARCHAR(60));'
        self.m.ddl(self.stmt)
        self.stmt = "INSERT INTO data(name) VALUES('{}');".format(phrase)
        self.m.dml(self.stmt)
        self.query = 'SELECT name FROM data;'
        self.assertEqual(phrase, self.m.dql(self.query))

    def ttest_19_date_format(self):
        # https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_current-timestamp
        # https://docs.python.org/2/library/datetime.html
        self.stmt = 'CREATE TABLE data(created CHAR(20));'
        self.m.ddl(self.stmt)
        self.stmt = "INSERT INTO data(created) VALUES (DATE_FORMAT(NOW(), '%b %d %Y %h:%i %p'));"
        self.m.dml(self.stmt)
        self.query = 'SELECT created FROM data;'
        expected = datetime.now().strftime('%b %d %Y %H:%M %p')
        #'May 11 2017 15:26 PM' != 'May 11 2017 10:26 PM'
        self.assertEqual(expected, self.m.dql(self.query))


    def ttest_19_date_format(self):
        """
        https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_current-timestamp
        https://docs.python.org/2/library/datetime.html
        """
        inputs = 'date_format'
        self.m.ddl(Fixture.load_schema(inputs))
        self.m.ddl(Fixture.populate(inputs))
        self.assertEqual(Decimal(Fixture.expected(inputs)),
                         self.m.dql(Fixture.test(inputs)))

        self.stmt = 'CREATE TABLE data(created DATE);'
        self.m.ddl(self.stmt)
        self.stmt = "INSERT INTO data(created) VALUES (DATE_FORMAT(NOW(), '%b %d %Y %h:%i %p'));"
        self.m.dml(self.stmt)
        self.query = 'SELECT created FROM data;'
        expected = datetime.now().strftime('%b %d %Y %H:%M %p')
        #'May 11 2017 15:26 PM' != 'May 11 2017 10:26 PM'
        self.assertEqual(expected, self.m.dql(self.query))

