from unittest import TestCase

from querygen import Filters


class TestFilters(TestCase):
    def test_simple_parser_empty(self):
        filters = Filters()
        name = 'fred'
        values = []
        result = filters.simple_parser(name, values)
        exp = ""
        self.assertEqual(result, exp, 'Empty value')

    def test_simple_parser_single_value(self):
        filters = Filters()
        name = 'fred'
        values = ['1']
        result = filters.simple_parser(name, values)
        exp = "fred = '1'"
        self.assertEqual(result, exp, 'Single value')

    def test_simple_parser_multiple_values(self):
        filters = Filters()
        name = 'fred'
        values = ['1', '2']
        result = filters.simple_parser(name, values)
        exp = "fred IN ('1', '2')"
        self.assertEqual(result, exp, 'Single value')

    def test_simple_parser_quoted(self):
        filters = Filters()
        name = 'fred'
        values = ["'1'", "'2'"]
        result = filters.simple_parser(name, values)
        exp = "fred IN ('1', '2')"
        self.assertEqual(result, exp, 'Single value')

    def test_query_parser_simple(self):
        filters = Filters()
        name = 'wilma'
        values = ['1', '2']
        result = filters.simple_parser(name, values)
        exp = "wilma IN ('1', '2')"
        self.assertEqual(result, exp, 'Query parser, simple values')

    def test_query_parser_query(self):
        filters = Filters()
        name = 'wilma'
        values = ['query = my query']
        result = filters.query_parser(name, values)
        exp = "wilma IN (my query)"
        self.assertEqual(result, exp, 'Query parser, query')

    def test_query_parser_query_multiple(self):
        filters = Filters()
        name = 'wilma'
        values = ['query = my query', 'query=other query']
        result = filters.query_parser(name, values)
        exp = "(wilma IN (my query) OR wilma IN (other query))"
        self.assertEqual(result, exp, 'Query parser, multiple queries')

    def test_query_parser_query_multiple_mixed(self):
        filters = Filters()
        name = 'wilma'
        values = ['a', 'query = my query', 'b', 'query=other query', 'c']
        result = filters.query_parser(name, values)
        exp = "(wilma IN ('a', 'b', 'c') OR wilma IN (my query) OR wilma IN (other query))"
        self.assertEqual(result, exp, 'Query parser, mixed')

    def test_project_parser(self):
        # TODO simply defers to simple_parser
        pass

    def test_languagecode_parser(self):
        # TODO simply defers to simple_parser
        pass

    def test_village_parser_simple(self):
        filters = Filters()
        name = 'village'
        values = ['QONOS']
        result = filters.village_parser(name, values)
        exp = "village = 'QONOS'"
        self.assertEqual(result, exp, 'Single village')

    def test_village_parser_empty(self):
        filters = Filters()
        name = 'village'
        values = []
        result = filters.village_parser(name, values)
        exp = "village != 'UNKNOWN'"
        self.assertEqual(result, exp, 'No village')

    def test_acm_categoryid_parser(self):
        # TODO simply defers to query_parser
        pass

    def test_deploymentnumber_parser(self):
        # TODO currently simply defers to simple_parser
        pass

    def test_add_filter_empty(self):
        filters = Filters()
        self.assertEqual(len(filters.column_filters), 1, 'Add no items')

    def test_add_filter_one(self):
        filters = Filters()
        v = filters.add_filter('project', ['XYZ'])
        self.assertTrue(v, 'Add item should return true')
        self.assertEqual(len(filters.column_filters), 2, 'Add one item')

    def test_add_filter_same(self):
        filters = Filters()
        filters.add_filter('project', ['XYZ'])
        filters.add_filter('project', ['XYZ'])
        self.assertEqual(len(filters.column_filters), 2, 'Add one item twice')

    def test_add_filter_add_then_remove(self):
        filters = Filters()
        filters.add_filter('project', ['XYZ'])
        filters.add_filter('project', [])
        self.assertEqual(len(filters.column_filters), 1, 'Add one item, then remove it')

    def test_add_filter_one_village(self):
        filters = Filters()
        filters.add_filter('village', ['XYZ'])
        self.assertEqual(len(filters.column_filters), 1, 'Add one item')

    def test_add_filter_same_village(self):
        filters = Filters()
        filters.add_filter('village', ['XYZ'])
        filters.add_filter('village', ['XYZ'])
        self.assertEqual(len(filters.column_filters), 1, 'Add one item twice')

    def test_add_filter_add_then_remove_village(self):
        filters = Filters()
        filters.add_filter('village', ['XYZ'])
        filters.add_filter('village', [])
        self.assertEqual(len(filters.column_filters), 1, 'Add one item, then remove it')

    def test_add_filter_add_undefined(self):
        filters = Filters()
        v = filters.add_filter('bogus', ['XYZ'])
        self.assertFalse(v, 'Attempt to add bogus filter should return false')
        self.assertEqual(len(filters.column_filters), 1, 'Add one bogus item')

    def test_get_filters_true(self):
        filters = Filters()
        result = filters.get_filters(lambda x: True)
        exp = "village != 'UNKNOWN'"
        self.assertEqual(result, exp, 'Get from empty filters, True')

    def test_get_filters_false(self):
        filters = Filters()
        result = filters.get_filters(lambda x: False)
        exp = ""
        self.assertEqual(result, exp, 'Get from empty filters, False')

    def test_get_filters_lambda(self):
        filters = Filters()
        result = filters.get_filters(lambda x: x=='village')
        exp = "village != 'UNKNOWN'"
        self.assertEqual(result, exp, 'Get from empty filters, lambda matches')
        result = filters.get_filters(lambda x: x == '(*^&*&%^')
        exp = ""
        self.assertEqual(result, exp, 'Get from empty filters, lambda does not match')

    def test_get_filters_after_add(self):
        filters = Filters()

        filters.add_filter('project', ['XYZ'])
        result = filters.get_filters(lambda x: True)
        # don't know what order they will be, but there should be 2 lines
        split = result.split('\n')
        self.assertEqual(len(split), 2, 'Filter with project, True, should have two phrases')

        filters.add_filter('categoryid', ['query = my query', 'query=other query'])
        result = filters.get_filters(lambda x: True)  # there is no column flavor
        # should be 3 lines
        split = result.split('\n')
        self.assertEqual(len(split), 3, 'Filter with project, 2 content queries, True, should have three phrases')
