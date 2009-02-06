from __future__ import absolute_import
import unittest
import datetime

import gitdb
import database

class NumbexDBPrimitiveTest(unittest.TestCase):
    def setUp(self):
        self.repo = gitdb.NumbexRepo('', lambda x: None)

    def testMakePath(self):
        f = self.repo.make_repo_path
        self.assertEqual(f('a'), 'a')
        self.assertEqual(f('aa'), 'aa')
        self.assertEqual(f(''), '')
        self.assertEqual(f('aaa'), 'aa/a')
        self.assertEqual(f('aaaa'), 'aa/aa')
        d = '48588887755'
        a = '48/58/88/87/75/5'
        self.assertEqual(f(d), a)

    def testMakeRecord(self):
        f = self.repo.make_record
        v = ['+48581234', '+48581999', 'sip.freeconet.pl', 'freeconet', '20090202T20:00:00', '']
        r = '''Range-start: +48581234
Range-end: +48581999
Sip-address: sip.freeconet.pl
Owner: freeconet
Date-modified: 20090202T20:00:00
Signature:'''
        self.assertEqual(f(*v).strip(), r)

    def testParseRecord(self):
        f = self.repo.parse_record
        r = ['+48581234', '+48581999', 'sip.freeconet.pl', 'freeconet', '20090202T20:00:00', '']
        v = '''Range-start: +48581234
Range-end: +48581999
Sip-address: sip.freeconet.pl
Owner: freeconet
Date-modified: 20090202T20:00:00
Signature:'''
        self.assertEqual(f(v), r)

class NumbexDBImportTest(unittest.TestCase):
    def setUp(self):
        self.db = database.Database(':memory:')
        self.db.create_db()
        self.db._populate_example()
        self.repo = gitdb.NumbexRepo('/tmp/testrepo', self.db.get_public_keys)

    def tearDown(self):
        import os
        os.system('rm -rf /tmp/testrepo')

    def testImportFailSig(self):
        # note bad signature
        data = [['+48581000', u'+48581999', u'sip.freeconet.pl',
            u'freeconet', datetime.datetime.now(), u'placeholder sig']]
        self.assertFalse(self.repo.import_data(data))

    def testImportSimple(self):
        # signature generated from key in database.py
        data = [['+48581000', u'+48581999', u'sip.freeconet.pl',
            u'freeconet', datetime.datetime.now(),
            'AAAAFFEWPmmz61zoQ5X8gbgx0drXwYYp AAAAFQCj+lvm4hueYmql6r1p87kmkaqfOw==']]
        self.assert_(self.repo.import_data(data))



if __name__ == '__main__':
    unittest.main()
