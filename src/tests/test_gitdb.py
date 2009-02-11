from __future__ import absolute_import
import unittest
import datetime
import os

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


class RepoDataMixin(object):
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
        record1 = ['+484000',
                 '+484999',
                 'sip.freeconet.pl',
                 'freeconet',
                 datetime.datetime(2009, 2, 10, 12, 15, 43, 903607),
                 'AAAAFQCn3vHSuLoxcsc9dJaFj+M989sNIQ== AAAAFAivBp7N8tBAQpXxeci3VQ3S0L8F']
        record2 = ['+484000',
                 '+484999',
                 'new.freeconet.pl',
                 'freeconet',
                 datetime.datetime(2009, 2, 10, 12, 22, 43, 67814),
                 'AAAAFF+4ccxb+ihQFxOUTjF3uddu7utk AAAAFEn8PyJE8CkkYLubyLgbhQRbyTBq']
        record3 = ['+485000',
                 '+485500',
                 'new.freeconet.pl',
                 'freeconet',
                 datetime.datetime(2009, 2, 11, 0, 15, 17, 457868),
                 'AAAAFQCWbSY9RGhJrFMroQnJhPtozMz2gA== AAAAFQCrULccnUwPwmQiIiv10oC26OdOKw==']
        self.record1 = record1
        self.record2 = record2
        self.record3 = record3
        self.data = data


class NumbexDBMergeTestBase(unittest.TestCase, RepoDataMixin):
    def setUp(self):
        # db is only needed for keys
        self.db = database.Database(':memory:')
        self.db.create_db()
        self.db._populate_example()
        os.system('rm -rf /tmp/testrepo1')
        os.system('rm -rf /tmp/testrepo2')
        self.repo1 = gitdb.NumbexRepo('/tmp/testrepo1', self.db.get_public_keys)
        self.repo2 = gitdb.NumbexRepo('/tmp/testrepo2', self.db.get_public_keys)
        self.setUpData()

    def setUpData(self):
        super(NumbexDBMergeTestBase, self).setUpData()

        self.repo1.import_data(self.data)
        self.repo1.sync()
        self.repo2.add_remote('repo1', self.repo1.repodir)
        self.repo2.fetch_from_remote('repo1')
        self.repo2.shelf.git('branch', self.repo1.repobranch, 'repo1/'+self.repo1.repobranch)
        self.repo2.reload()

    def tearDown(self):
        os.system('rm -rf /tmp/testrepo1')
        os.system('rm -rf /tmp/testrepo2')


class NumbexDBMergeTest1(NumbexDBMergeTestBase):
    def test_merge(self):
        self.repo1.import_data([self.record1])
        self.repo1.sync()
        self.repo2.import_data([self.record2, self.record3])
        self.repo2.sync()
        self.repo2.fetch_from_remote('repo1')
        self.repo2.merge('repo1/'+self.repo1.repobranch)
        self.repo2.reload()
        self.assertEqual(self.repo2.get_range('+484000'), self.record2)
        self.assertEqual(self.repo2.get_range('+485000'), self.record3)
        self.assertEqual(self.repo2.get_range('+481000'), self.data[0])

class NumbexDBMergeTest2(NumbexDBMergeTestBase):
    def test_merge(self):
        self.repo1.import_data([self.record1])
        self.repo1.sync()
        self.repo1.import_data([self.record2, self.record3])
        self.repo1.sync()
        self.repo2.fetch_from_remote('repo1')
        self.repo2.merge('repo1/'+self.repo1.repobranch)
        self.repo2.reload()
        self.assertEqual(self.repo2.get_range('+484000'), self.record2)
        self.assertEqual(self.repo2.get_range('+485000'), self.record3)
        self.assertEqual(self.repo2.get_range('+481000'), self.data[0])

class NumbexDBMergeTestEmpty(NumbexDBMergeTestBase):
    def test_merge(self):
        self.repo2.fetch_from_remote('repo1')
        self.repo2.merge('repo1/'+self.repo1.repobranch)
        self.repo2.reload()
        self.assertEqual(self.repo2.get_range('+481000'), self.data[0])
        self.assertEqual(self.repo2.get_range('+482500'), self.data[1])

class NumbexDBMergeTest3(NumbexDBMergeTestBase):
    def setUpData(self):
        RepoDataMixin.setUpData(self)

        self.repo1.import_data(self.data)
        self.repo1.sync()
        self.repo2.add_remote('repo1', self.repo1.repodir)
        self.repo2.import_data([self.record1])
        self.repo2.sync()

    def test_no_common_ancestor(self):
        self.repo2.fetch_from_remote('repo1')
        self.repo2.merge('repo1/'+self.repo1.repobranch)
        self.repo2.reload()
        self.assertEqual(self.repo2.get_range('+481000'), self.data[0])
        self.assertEqual(self.repo2.get_range('+482500'), self.data[1])
        self.assertEqual(self.repo2.get_range('+484000'), self.record1)


if __name__ == '__main__':
    unittest.main()
