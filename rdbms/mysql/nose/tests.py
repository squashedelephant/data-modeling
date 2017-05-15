#!/usr/bin/env python 

from unittest import TestCase, TestLoader, TestSuite, TextTestRunner

from test_mysql_datatype import TestMySQLDataType
from test_mysql_function import TestMySQLFunction
from test_mysql_relationship_model import TestMySQLRelationshipModel
from test_mysql_table_join import TestMySQLTableJoin
from test_mysql_table_select import TestMySQLTableSelect

def my_suite():
    suite = TestSuite()
    loader = TestLoader()
    suite.addTest(loader.loadTestsFromTestCase(TestMySQLDataType))
    suite.addTest(loader.loadTestsFromTestCase(TestMySQLFunction))
    suite.addTest(loader.loadTestsFromTestCase(TestMySQLTableSelect))
    suite.addTest(loader.loadTestsFromTestCase(TestMySQLTableJoin))
    suite.addTest(loader.loadTestsFromTestCase(TestMySQLRelationshipModel))
    return suite

if __name__ == '__main__':
    runner = TextTestRunner()
    runner.run(my_suite())
