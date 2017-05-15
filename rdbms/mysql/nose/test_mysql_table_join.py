
from datetime import date, datetime, timedelta
from pytz import UTC
from _mysql_exceptions import DataError, OperationalError, ProgrammingError
from unittest import TestCase

from my_mysql import MyMySQL

class TestMySQLTableJoin(TestCase):
    """
    """
    def setUp(self):
        self.m = MyMySQL()
        self.stmt = None
        self.query = None

    def tearDown(self):
        self.stmt = 'DROP TABLE IF EXISTS profile;'
        self.m.ddl(self.stmt)
        self.stmt = 'DROP TABLE IF EXISTS user;'
        self.m.ddl(self.stmt)
        self.stmt = 'DROP TABLE IF EXISTS purchase_order;'
        self.m.ddl(self.stmt)
        self.stmt = 'DROP TABLE IF EXISTS customer;'
        self.m.ddl(self.stmt)
        self.m = None

    def test_01_inner_join(self):
        self.stmt = 'CREATE TABLE user(id INT NOT NULL AUTO_INCREMENT, ' +\
                    'name CHAR(15) NOT NULL, PRIMARY KEY (id));'
        self.m.ddl(self.stmt)
        self.stmt = 'CREATE TABLE profile(id INT NOT NULL AUTO_INCREMENT, ' +\
                    'user_id INT, password VARCHAR(30), email VARCHAR(50), ' +\
                    'PRIMARY KEY (id), INDEX user_idx (user_id), FOREIGN ' +\
                    'KEY (user_id) REFERENCES user(id) ON DELETE CASCADE);'
        self.m.ddl(self.stmt)
        self.stmt = "INSERT INTO user(name) VALUES('Bill Gates'), " +\
                    "('Steve Jobs'), ('Larry Ellison');"
        self.m.ddl(self.stmt)
        self.stmt = "INSERT INTO profile SET user_id = (SELECT id FROM " +\
                    "user WHERE name = 'Bill Gates'), " +\
                    "password = 'Microsoft', email = 'bill_gates@microsoft.com';"
        self.m.dml(self.stmt)
        self.stmt = "INSERT INTO profile SET user_id = (SELECT id FROM " +\
                    "user WHERE name = 'Steve Jobs'), " +\
                    "password = 'Apple', email = 'steve_jobs@apple.com';"
        self.m.dml(self.stmt)
        self.stmt = "INSERT INTO profile SET user_id = (SELECT id FROM " +\
                    "user WHERE name = 'Larry Ellison'), " +\
                    "password = 'Oracle', email = 'larry_ellison@oracle.com';"
        self.m.dml(self.stmt)
        self.query = 'SELECT u.name, p.password, p.email FROM user AS u ' +\
                     'INNER JOIN profile as p ON u.id = p.user_id;'
        rows = self.m.dql(self.query)
        self.assertEqual('Bill Gates', rows[0][0][0]);
        self.assertEqual('Microsoft', rows[0][0][1]);
        self.assertEqual('bill_gates@microsoft.com', rows[0][0][2]);
        self.assertEqual('Steve Jobs', rows[1][0][0]);
        self.assertEqual('Apple', rows[1][0][1]);
        self.assertEqual('steve_jobs@apple.com', rows[1][0][2]);
        self.assertEqual('Larry Ellison', rows[2][0][0]);
        self.assertEqual('Oracle', rows[2][0][1]);
        self.assertEqual('larry_ellison@oracle.com', rows[2][0][2]);

    def test_02_left_join(self):
        self.stmt = 'CREATE TABLE customer(id INT NOT NULL AUTO_INCREMENT, ' +\
                    'name CHAR(15) NOT NULL, contact CHAR(30) NOT NULL, ' +\
                    'PRIMARY KEY (id));'
        self.m.ddl(self.stmt)
        self.stmt = 'CREATE TABLE purchase_order(id INT NOT NULL AUTO_INCREMENT, ' +\
                    'submitted DATE, required DATE, shipped DATE, ' +\
                    'customer_id INT, PRIMARY KEY (id), INDEX cust_idx ' +\
                    '(customer_id), FOREIGN KEY (customer_id) ' +\
                    'REFERENCES customer(id) ON DELETE CASCADE);'
        self.m.dml(self.stmt)
        self.stmt = "INSERT INTO customer(name, contact) " +\
                    "VALUES('Microsoft', 'Bill Gates'), " +\
                    "('Apple', 'Steve Jobs'), ('Oracle', 'Larry Ellison');"
        self.m.dml(self.stmt)
        self.stmt = "INSERT INTO purchase_order SET submitted = CURRENT_DATE(), " +\
                    "required = CURRENT_DATE(), shipped = CURRENT_DATE(), " +\
                    "customer_id = (SELECT id FROM customer WHERE name = 'Microsoft');"
        self.m.dml(self.stmt)
        self.stmt = "INSERT INTO purchase_order SET submitted = CURRENT_DATE(), " +\
                    "required = DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY), " +\
                    "shipped = DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY), " +\
                    "customer_id = (SELECT id FROM customer WHERE name = 'Apple');"
        self.m.dml(self.stmt)
        self.query = 'SELECT c.name, po.id, po.shipped FROM customer AS c ' +\
                     'LEFT JOIN purchase_order AS po ON c.id = po.customer_id;'
        rows = self.m.dql(self.query)
        self.assertEqual('Microsoft', rows[0][0][0]);
        self.assertEqual(1, rows[0][0][1]);
        today = date.today()
        self.assertEqual(today, rows[0][0][2]);
        self.assertEqual('Apple', rows[1][0][0]);
        self.assertEqual(2, rows[1][0][1]);
        tomorrow = date.today() + timedelta(days=1)
        self.assertEqual(tomorrow, rows[1][0][2]);
        self.assertEqual('Oracle', rows[2][0][0]);
        self.assertEqual(None, rows[2][0][1]);
        self.assertEqual(None, rows[2][0][2]);

    def test_03_right_join(self):
        pass

    def test_04_full_outer_join(self):
        pass

    def test_05_self_join(self):
        pass

