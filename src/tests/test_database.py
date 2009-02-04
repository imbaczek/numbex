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


class TestDBInsertOverlap(unittest.TestCase):
    def setUp(self):
        self.db = database.Database(':memory:', fill_example=False)
        self.db.create_db()
        v1 = (u'+48581234', u'+48581999', u'sip.freeconet.pl',
            u'freeconet', datetime.datetime.now(), u'')
        c = self.db.conn.cursor()
        self.db.insert_range(c, *v1)
        c.close()
        self.cursor = self.db.conn.cursor()

    def tearDown(self):
        self.cursor.close()
         
    def test_overlap_inner(self):
        v2 = (u'+48581300', u'+48581500', u'sip.freeconet.pl',
            u'freeconet', datetime.datetime.now(), u'')
        self.assertRaises(ValueError, self.db.insert_range, self.cursor, *v2)

    def test_overlap_outer(self):
        v2 = (u'+48581200', u'+48582500', u'sip.freeconet.pl',
            u'freeconet', datetime.datetime.now(), u'')
        self.assertRaises(ValueError, self.db.insert_range, self.cursor, *v2)

    def test_overlap_left(self):
        v2 = (u'+48581200', u'+48581300', u'sip.freeconet.pl',
            u'freeconet', datetime.datetime.now(), u'')
        self.assertRaises(ValueError, self.db.insert_range, self.cursor, *v2)

    def test_overlap_right(self):
        v2 = (u'+48581900', u'+48582500', u'sip.freeconet.pl',
            u'freeconet', datetime.datetime.now(), u'')
        self.assertRaises(ValueError, self.db.insert_range, self.cursor, *v2)



if __name__ == '__main__':
    unittest.main()
