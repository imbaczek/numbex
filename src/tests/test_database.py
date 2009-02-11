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
        r = self.db.get_range('+48581234')
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


class TestDBSignatures(unittest.TestCase):
    def setUp(self):
        self.db = database.Database(':memory:')
        self.db.create_db()
        self.db._populate_example()

    def test_check_data_sigs(self):
        data = self.db.get_data_all()
        self.assert_(self.db.check_data_signatures(data))


class TestDBUpdateData(unittest.TestCase):
    def setUp(self):
        self.db = database.Database(':memory:')

    def singleSetUp(self):
        self.db.create_db()
        v = (u'+48581000', u'+48581999', u'sip.freeconet.pl',
            u'freeconet', datetime.datetime.now(), u'')
        self.rec = v
        cursor = self.db.conn.cursor()
        self.db.insert_range(cursor, *v)
        cursor.close()

    def singleTearDown(self):
        self.db.drop_db()

    def update_data_test(self, data, expected):
        self.db.update_data(data)
        # we'll have to ignore the mdate
        result = [[s, e, sip, owner, None, sig] 
                for s, e, sip, owner, mdate, sig in self.db.get_data_all()]
        self.assertEqual(sorted(result, key=lambda x: int(x[0])), expected)

    def test_equal_overlap(self):
        self.singleSetUp()
        data = [['+48581000', u'+48581999', u'sip.freeconet.pl',
            u'freeconet', datetime.datetime.now(), u'placeholder sig']]
        result = [['+48581000', u'+48581999', u'sip.freeconet.pl',
            u'freeconet', None, u'placeholder sig']]
        self.update_data_test(data, result)
        self.singleTearDown()

    def test_inner_overlap(self):
        self.singleSetUp()
        data = [(u'+48581001', u'+48581998', u'new.freeconet.pl',
            u'freeconet', datetime.datetime.now(), u'some sig')]
        expected = [
        [u'+48581000',u'+48581000', u'sip.freeconet.pl',u'freeconet',None,u''],
        [u'+48581001',u'+48581998', u'new.freeconet.pl',u'freeconet',None,u'some sig'],
        [u'+48581999',u'+48581999', u'sip.freeconet.pl',u'freeconet',None,u''],
        ]
        self.update_data_test(data, expected)
        self.singleTearDown()

    def test_left_overlap(self):
        self.singleSetUp()
        data = [(u'+4858999', u'+48581000', u'new.freeconet.pl',
            u'freeconet', datetime.datetime.now(), u'some sig')]
        expected = [
        [u'+4858999',u'+48581000',  u'new.freeconet.pl',u'freeconet',None,u'some sig'],
        [u'+48581001',u'+48581999', u'sip.freeconet.pl',u'freeconet',None,u''],
        ]
        self.update_data_test(data, expected)
        self.singleTearDown()

    def test_right_overlap(self):
        self.singleSetUp()
        data = [(u'+48581999', u'+48582000', u'new.freeconet.pl',
            u'freeconet', datetime.datetime.now(), u'some sig')]
        expected = [
        [u'+48581000',u'+48581998', u'sip.freeconet.pl',u'freeconet',None,u''],
        [u'+48581999',u'+48582000', u'new.freeconet.pl',u'freeconet',None,u'some sig'],
        ]
        self.update_data_test(data, expected)
        self.singleTearDown()

    def test_outer_overlap(self):
        self.singleSetUp()
        data = [(u'+4858999', u'+48582000', u'new.freeconet.pl',
            u'freeconet', datetime.datetime.now(), u'some sig')]
        expected = [
        [u'+4858999',u'+48582000', u'new.freeconet.pl',u'freeconet',None,u'some sig'],
        ]
        self.update_data_test(data, expected)
        self.singleTearDown()

    def test_right_outer_overlap(self):
        self.singleSetUp()
        data = [(u'+48581000', u'+48582000', u'new.freeconet.pl',
            u'freeconet', datetime.datetime.now(), u'some sig')]
        expected = [
        [u'+48581000',u'+48582000', u'new.freeconet.pl',u'freeconet',None,u'some sig'],
        ]
        self.update_data_test(data, expected)
        self.singleTearDown()

    def test_left_outer_overlap(self):
        self.singleSetUp()
        data = [(u'+4858999', u'+48581999', u'new.freeconet.pl',
            u'freeconet', datetime.datetime.now(), u'some sig')]
        expected = [
        [u'+4858999',u'+48581999', u'new.freeconet.pl',u'freeconet',None,u'some sig'],
        ]
        self.update_data_test(data, expected)
        self.singleTearDown()


if __name__ == '__main__':
    unittest.main()
