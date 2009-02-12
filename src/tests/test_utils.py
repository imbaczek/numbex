from __future__ import absolute_import
import unittest
import datetime

import utils

class TestParseIsoDate(unittest.TestCase):
    def test_parse1(self):
        f = utils.parse_datetime_iso
        y = datetime.datetime(2009, 2, 2, 2, 2, 2)
        x = '2009-02-02T02:02:02'
        self.assertEqual(f(x), y)

    def test_parse2(self):
        f = utils.parse_datetime_iso
        y = datetime.datetime(2009, 2, 2, 2, 2, 2, 2222)
        x = '2009-02-02T02:02:02.002222'
        self.assertEqual(f(x), y)

if __name__ == '__main__':
    unittest.main()
