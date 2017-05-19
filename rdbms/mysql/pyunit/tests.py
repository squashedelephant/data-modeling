#!/usr/bin/env python 

from unittest import TestCase, TestLoader, TestSuite, TextTestRunner

from features.test_mysql_datatype import TestMySQLDataType
from features.test_mysql_function import TestMySQLFunction
from tables.test_mysql_table_join import TestMySQLTableJoin
from tables.test_mysql_table_constraint import TestMySQLTableConstraint
from tables.test_mysql_table_delete import TestMySQLTableDelete
from tables.test_mysql_table_select import TestMySQLTableSelect
from tables.test_mysql_table_select_group_by import TestMySQLTableSelectGroupBy
from tables.test_mysql_table_select_order_by import TestMySQLTableSelectOrderBy
from tables.test_mysql_table_trigger import TestMySQLTableTrigger
from tables.test_mysql_table_update import TestMySQLTableUpdate
from use_case.test_mysql_relationship_model import TestMySQLRelationshipModel

def my_suite():
    suite = TestSuite()
    loader = TestLoader()
    suite.addTest(loader.loadTestsFromTestCase(TestMySQLDataType))
    suite.addTest(loader.loadTestsFromTestCase(TestMySQLFunction))
    suite.addTest(loader.loadTestsFromTestCase(TestMySQLRelationshipModel))
    suite.addTest(loader.loadTestsFromTestCase(TestMySQLTableConstraint))
    suite.addTest(loader.loadTestsFromTestCase(TestMySQLTableDelete))
    suite.addTest(loader.loadTestsFromTestCase(TestMySQLTableJoin))
    suite.addTest(loader.loadTestsFromTestCase(TestMySQLTableSelect))
    suite.addTest(loader.loadTestsFromTestCase(TestMySQLTableSelectGroupBy))
    suite.addTest(loader.loadTestsFromTestCase(TestMySQLTableSelectOrderBy))
    suite.addTest(loader.loadTestsFromTestCase(TestMySQLTableTrigger))
    suite.addTest(loader.loadTestsFromTestCase(TestMySQLTableUpdate))
    return suite

if __name__ == '__main__':
    runner = TextTestRunner(verbosity=2)
    runner.run(my_suite())
