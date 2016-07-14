from unittest import TestCase

from querygen import enquote


class TestEnquote(TestCase):
    def testEmpty(self):
        inp = []
        outp = enquote(inp)
        self.assertEqual(inp, outp, 'Empty array should come back empty')

    def testAddingQuotes(self):
        inp = ["a", "b", "c"]
        exp = ["'a'", "'b'", "'c'"]
        outp = enquote(inp)
        self.assertEqual(outp, exp, 'Adding quotes to values')

    def testAlreadyQuoted(self):
        inp = ["'a'", "'b'", "'c'"]
        outp = enquote(inp)
        self.assertEqual(inp, outp, 'Already quoted should come back equal')

