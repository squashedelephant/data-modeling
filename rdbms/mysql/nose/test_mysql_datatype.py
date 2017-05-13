
from binascii import b2a_hex
from datetime import date, datetime, timedelta
from decimal import Decimal
from pytz import UTC
from _mysql_exceptions import DataError, OperationalError, ProgrammingError
from unittest import TestCase

from my_mysql import MyMySQL
from my_utils import Fixture

class TestMySQLDataType(TestCase):
    def setUp(self):
        self.m = MyMySQL()
        self.f = Fixture('fixtures/datatype')
        

    def tearDown(self):
        self.m.ddl(self.f.drop_schema('datatype'))
        self.m = None
        self.f = None

    def test_01_bit_min(self):
        inputs = 'bit_min'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(int(self.f.expected(inputs)),
                         int(b2a_hex(self.m.dql(self.f.test(inputs))), 16))

    def test_02_bit_max(self):
        inputs = 'bit_max'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(int(self.f.expected(inputs)),
                         int(b2a_hex(self.m.dql(self.f.test(inputs))), 16))

    def test_03_bit_range_exceeded(self):
        inputs = 'bit_range_exceeded'
        self.m.ddl(self.f.load_schema(inputs))
        with self.assertRaises(DataError) as context:
            self.m.ddl(self.f.populate(inputs))
        self.assertIn('Data too long for column', str(context.exception))

    def test_04_tinyint_signed_min(self):
        inputs = 'tinyint_signed_min'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(int(self.f.expected(inputs)),
                         self.m.dql(self.f.test(inputs)))

    def test_05_tinyint_signed_max(self):
        inputs = 'tinyint_signed_max'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(int(self.f.expected(inputs)),
                         self.m.dql(self.f.test(inputs)))

    def test_06_smallint_signed_min(self):
        inputs = 'smallint_signed_min'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(int(self.f.expected(inputs)),
                         self.m.dql(self.f.test(inputs)))

    def test_07_smallint_signed_max(self):
        inputs = 'smallint_signed_max'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(int(self.f.expected(inputs)),
                         self.m.dql(self.f.test(inputs)))

    def test_08_mediumint_signed_min(self):
        inputs = 'mediumint_signed_min'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(int(self.f.expected(inputs)),
                         self.m.dql(self.f.test(inputs)))

    def test_09_mediumint_signed_max(self):
        inputs = 'mediumint_signed_max'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(int(self.f.expected(inputs)),
                         self.m.dql(self.f.test(inputs)))

    def test_10_int_signed_min(self):
        inputs = 'int_signed_min'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(int(self.f.expected(inputs)),
                         self.m.dql(self.f.test(inputs)))

    def test_11_int_signed_max(self):
        inputs = 'int_signed_max'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(int(self.f.expected(inputs)),
                         self.m.dql(self.f.test(inputs)))

    def test_12_bigint_signed_min(self):
        inputs = 'bigint_signed_min'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(int(self.f.expected(inputs)),
                         self.m.dql(self.f.test(inputs)))

    def test_13_bigint_signed_max(self):
        inputs = 'bigint_signed_max'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(int(self.f.expected(inputs)),
                         self.m.dql(self.f.test(inputs)))

    def test_14_decimal_round_off(self):
        inputs = 'decimal_round_off'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(Decimal(self.f.expected(inputs)),
                         self.m.dql(self.f.test(inputs)))

    def test_15_decimal_round_up(self):
        inputs = 'decimal_round_up'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(Decimal(self.f.expected(inputs)),
                         self.m.dql(self.f.test(inputs)))

    def test_16_decimal_round_down(self):
        inputs = 'decimal_round_down'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(Decimal(self.f.expected(inputs)),
                         self.m.dql(self.f.test(inputs)))

    def test_17_float_round_off(self):
        inputs = 'float_round_off'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(Decimal(self.f.expected(inputs)),
                         self.m.dql(self.f.test(inputs)))

    def test_18_float_precision(self):
        inputs = 'float_precision'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(float(self.f.expected(inputs)),
                         self.m.dql(self.f.test(inputs)))

    def test_19_date_min(self):
        """
        https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_current-timestamp
        https://docs.python.org/2/library/datetime.html
        """
        inputs = 'date_min'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(self.f.expected(inputs),
                         str(self.m.dql(self.f.test(inputs))))

    def test_20_date_max(self):
        """
        https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_current-timestamp
        https://docs.python.org/2/library/datetime.html
        """
        inputs = 'date_max'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(self.f.expected(inputs),
                         str(self.m.dql(self.f.test(inputs))))

    def test_21_time_min(self):
        """
        https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_current-timestamp
        https://docs.python.org/2/library/datetime.html
        """
        inputs = 'time_min'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(self.f.expected(inputs),
                         str(self.m.dql(self.f.test(inputs))))

    def test_22_time_max(self):
        """
        https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_current-timestamp
        https://docs.python.org/2/library/datetime.html
        """
        inputs = 'time_max'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(self.f.expected(inputs),
                         str(self.m.dql(self.f.test(inputs))))

    def test_23_datetime_min(self):
        """
        https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_current-timestamp
        https://docs.python.org/2/library/datetime.html
        """
        inputs = 'datetime_min'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(self.f.expected(inputs),
                         str(self.m.dql(self.f.test(inputs))))

    def test_24_datetime_max(self):
        """
        https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_current-timestamp
        https://docs.python.org/2/library/datetime.html
        """
        inputs = 'datetime_max'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(self.f.expected(inputs),
                         str(self.m.dql(self.f.test(inputs))))

    def test_25_timestamp_min(self):
        """
        https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_current-timestamp
        https://docs.python.org/2/library/datetime.html
        """
        inputs = 'timestamp_min'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(self.f.expected(inputs),
                         str(self.m.dql(self.f.test(inputs))))

    def test_26_timestamp_max(self):
        """
        https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_current-timestamp
        https://docs.python.org/2/library/datetime.html
        """
        inputs = 'timestamp_max'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(self.f.expected(inputs),
                         str(self.m.dql(self.f.test(inputs))))

    def test_27_char_min(self):
        inputs = 'char_min'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(self.f.expected(inputs),
                         self.m.dql(self.f.test(inputs)))

    def test_28_char_max(self):
        inputs = 'char_max'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(self.f.expected(inputs),
                         self.m.dql(self.f.test(inputs)))

    def test_29_char_trailing_spaces(self):
        inputs = 'char_trailing_spaces'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(self.f.expected(inputs),
                         self.m.dql(self.f.test(inputs)))

    def test_30_varchar_min(self):
        inputs = 'varchar_min'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(self.f.expected(inputs),
                         self.m.dql(self.f.test(inputs)))

    def test_31_varchar_max(self):
        inputs = 'varchar_max'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(''.join(self.f.expected(inputs)),
                         self.m.dql(self.f.test(inputs)))

    def test_32_text_min(self):
        inputs = 'text_min'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(''.join(self.f.expected(inputs)),
                         self.m.dql(self.f.test(inputs)))

    def test_33_text_max(self):
        inputs = 'text_max'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(''.join(self.f.expected(inputs)),
                         self.m.dql(self.f.test(inputs)))

    def test_34_enum(self):
        inputs = 'enum'
        self.m.ddl(self.f.load_schema(inputs))
        self.m.ddl(self.f.populate(inputs))
        self.assertEqual(''.join(self.f.expected(inputs)),
                         self.m.dql(self.f.test(inputs)))
