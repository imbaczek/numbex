from __future__ import absolute_import
import unittest
import datetime

import crypto
import gitdb
import database

class NumbexDBPrimitiveTest(unittest.TestCase):
    def setUp(self):
        self.repo = gitdb.NumbexRepo('', lambda x: None)

    def testMakePath(self):
        f = self.repo.make_repo_path
        self.assertEqual(f(''), '')
        self.assertEqual(f('a'), 'a')
        self.assertEqual(f('aa'), 'aa')
        self.assertEqual(f('aaa'), 'aaa')
        self.assertEqual(f('aaaa'), 'aaa/a')
        self.assertEqual(f('aaaaa'), 'aaa/aa')
        self.assertEqual(f('aaaaaa'), 'aaa/aaa')
        self.assertEqual(f('aaaaaaa'), 'aaa/aaa/a')
        d = '48588887755'
        a = '485/888/877/55'
        self.assertEqual(f(d), a)

    def testMakeRecord(self):
        f = self.repo.make_record
        v = ['+48581234', '+48581999', 'sip.freeconet.pl', 'freeconet', '2009-02-02T20:00:00', '']
        r = '''Range-start: +48581234
Range-end: +48581999
Sip-address: sip.freeconet.pl
Owner: freeconet
Date-modified: 2009-02-02T20:00:00
Signature:'''
        self.assertEqual(f(*v).strip(), r)

    def testParseRecord(self):
        f = self.repo.parse_record
        r = ['+48581234', '+48581999', 'sip.freeconet.pl', 'freeconet',
                datetime.datetime(2009, 2, 2, 20), '']
        v = '''Range-start: +48581234
Range-end: +48581999
Sip-address: sip.freeconet.pl
Owner: freeconet
Date-modified: 2009-02-02T20:00:00
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
        data = [[u'+48581000', u'+48581999', u'sip.freeconet.pl',
            u'freeconet', datetime.datetime(2009, 02, 02, 02, 02, 02),
            'AAAAFQCqNJnApi3/eDHb8jLqZptQ3Bn1Dg== AAAAFFe29PtuKNgB5wuC+S51q6ToBLuw'
]]
        self.assert_(self.repo.import_data(data))
        self.repo.sync()
        self.assertEqual(self.repo.get_range('+48581000'), data[0])
        self.assertEqual(self.repo.export_data_all(), data)

class NumbexDBCloneTest(unittest.TestCase):
    def setUp(self):
        # db is only needed for keys
        self.db = database.Database(':memory:')
        self.db.create_db()
        self.db._populate_example()
        self.repo1 = gitdb.NumbexRepo('/tmp/testrepo1', self.db.get_public_keys)
        self.repo2 = gitdb.NumbexRepo('/tmp/testrepo2', self.db.get_public_keys)
        self.setUpData()

    def setUpData(self):
        data = [['+481000',
                 '+481500',
                 'sip.freeconet.pl',
                 'freeconet',
                 datetime.datetime(2009, 2, 9, 16, 51, 20, 322133),
                 'AAAAFQCHHDGoHCotDhRjwamVnIXVrehHag== AAAAFA+Ov6nj5V2LugDEHOhfAwDvSb/1'],
                ['+482500',
                 '+483000',
                 'sip.freeconet.pl',
                 'freeconet',
                 datetime.datetime(2009, 2, 9, 19, 31, 48, 590016),
                 'AAAAFFA1HQw+i7ZIRCoECkDDy3ehCYHc AAAAFFoaeviJLijxAHb0qFfLyUjJXX/T']
            ]
        self.repo1.import_data(data)
        self.repo1.sync()

    def tearDown(self):
        import os
        os.system('rm -rf /tmp/testrepo1')
        os.system('rm -rf /tmp/testrepo2')



if __name__ == '__main__':
    unittest.main()
