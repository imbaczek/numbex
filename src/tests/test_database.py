from __future__ import absolute_import
import unittest
import datetime
import database

class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.db = database.Database(':memory:', fill_example=False)
        self.db.create_db()

    def test_insert(self):
        v = (u'+48581234', u'+48581999', u'sip.freeconet.pl',
            u'freeconet', datetime.datetime.now(), u'')
        c1 = self.db.conn.cursor()
        self.db.insert_range(c1, *v)
        c1.close()
        c2 = self.db.conn.cursor()
        r = self.db.get_range(c2, '+48581234')
        c2.close()
        self.assertEqual(v, r)


if __name__ == '__main__':
    unittest.main()
