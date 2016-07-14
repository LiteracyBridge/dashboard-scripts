from unittest import TestCase

from querygen import Report


class TestReport(TestCase):
    def test_create_report(self):
        lines = ['name = a-report', 'categoryname']
        report = Report('name', lines)
        self.assertTrue(len(report.columns) == 1, 'Should have one column')

    def test_create_usage_with_village(self):
        lines = ['name = a-report', 'village']
        report = Report('name', lines)
        self.assertTrue(len(report.columns) == 1, 'Should have one column')

    def test_create_deployment_with_village(self):
        lines = ['name = a-report', 'village']
        lines.append('type=deployment')
        report = Report('name', lines)
        self.assertTrue(len(report.columns) == 0, 'Deployment should have no columns')
