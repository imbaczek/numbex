from __future__ import absolute_import
import unittest
import datetime
import os
import time
import logging

from gitshelve import GitError

import crypto
import gitdb
import database

class NumbexDBPrimitiveTest(unittest.TestCase):
    def setUp(self):
        self.repo = gitdb.NumbexRepo('', lambda x: None)

    def testMakePath(self):
        f = self.repo.make_repo_path
        self.assertEqual(f(''), '/this')
        self.assertEqual(f('a'), 'a')
        self.assertEqual(f('aa'), 'aa')
        self.assertEqual(f('aaa'), 'aaa/this')
        self.assertEqual(f('aaaa'), 'aaa/a')
        self.assertEqual(f('aaaaa'), 'aaa/aa')
        self.assertEqual(f('aaaaaa'), 'aaa/aaa/this')
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
        record4 = ['+481000',
                '+485678',
                'sip.freeconet.pl',
                'freeconet',
                datetime.datetime(2009, 2, 12, 12, 00, 00, 20406),
                'AAAAFQCDaqtzhSvtTsqPEmjFdMBguNAxGw== AAAAFHqQvJlbHdp8aUScDZHUFlILeCQo']
        record5 = ['+481000',
                '+481500',
                 'sip.freeconet.pl',
                 'freeconet',
                 datetime.datetime(2009, 2, 10, 16, 51, 20, 999999),
                 'AAAAFD1tMMcsGbtf+EvwFPzgMEpfODLK AAAAFD/UHQJHHHwO/gwsCUtt2a3J2cSD']
        record6 = ['+482000',
                 '+483000',
                 'sip.freeconet.pl',
                 'freeconet',
                 datetime.datetime(2009, 2, 13, 19, 31, 48, 590016),
                 'AAAAFAau3ls3DXbRubC88WK9pKFJS8nd AAAAFAwDyBlcYwxGyHoyvkMzF8R/FqlL']




        self.record1 = record1
        self.record2 = record2
        self.record3 = record3
        self.record4 = record4
        self.record5 = record5
        self.record6 = record6
        self.data = data


class NumbexDBExportTest(unittest.TestCase, RepoDataMixin):
    def setUp(self):
        self.db = database.Database(':memory:')
        self.db.create_db()
        self.db._populate_example()
        os.system('rm -rf /tmp/testrepo1')
        self.repo1 = gitdb.NumbexRepo('/tmp/testrepo1', self.db.get_public_keys)
        self.setUpData()

    def setUpData(self):
        RepoDataMixin.setUpData(self)
        self.result = self.data + [self.record1, self.record3]
        self.repo1.import_data(self.data)
        self.repo1.import_data([self.record1, self.record3])
        self.repo1.sync()

    def test_export_all(self):
        self.assertEqual(self.repo1.export_data_all(), self.result)

    def test_export_since(self):
        since = datetime.datetime(2009, 2, 10, 0)
        self.assertEqual(self.repo1.export_data_since(since),
                [self.record1, self.record3])

    def test_start_daemon(self):
        self.repo1.start_daemon(11223)
        self.repo1.add_remote('self', 'git://localhost:11223/')
        try:
            self.repo1.fetch_from_remote('self')
        except:
            self.assert_(False)
        else:
            self.assert_(True)
        self.repo1.stop_daemon()
        self.assertRaises(GitError, self.repo1.fetch_from_remote, 'self')


    def tearDown(self):
        os.system('rm -rf /tmp/testrepo1')

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
        self.repo1.add_remote('repo2', self.repo2.repodir)
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
        self.assertFalse(self.repo2.check_overlaps())

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
        self.assertFalse(self.repo2.check_overlaps())

class NumbexDBMergeTestEmpty(NumbexDBMergeTestBase):
    def test_merge(self):
        self.repo2.fetch_from_remote('repo1')
        self.repo2.merge('repo1/'+self.repo1.repobranch)
        self.repo2.reload()
        self.assertEqual(self.repo2.get_range('+481000'), self.data[0])
        self.assertEqual(self.repo2.get_range('+482500'), self.data[1])
        self.assertFalse(self.repo2.check_overlaps())

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
        self.assertFalse(self.repo2.check_overlaps())

class NumbexDBMergeTest4(NumbexDBMergeTestBase):
    def test_merge(self):
        r = ['+481250',
            '+482250',
            'new.freeconet.pl',
            'freeconet',
            datetime.datetime(2009, 2, 12, 15, 00, 00, ),
            'AAAAFGJKpLrYGdiZnB2W8zf70SbLLC8/ AAAAFQCD1ev1NX3IfCXgWICcGW76/KX6XA==']
        self.repo1.import_data([r], delete=['+481000'])
        self.repo1.sync()
        self.repo2.import_data([self.record5, self.record3])
        self.repo2.sync()
        self.repo2.fetch_from_remote('repo1')
        self.repo2.merge('repo1/'+self.repo1.repobranch)
        self.repo2.reload()
        self.assertEqual(self.repo2.get_range('+485000'), self.record3)
        self.assertEqual(self.repo2.get_range('+481250'), r)
        self.assertRaises(KeyError, self.repo2.get_range, '+481000')
        self.assertRaises(KeyError, self.repo2.get_range, '+482000')
        self.assertFalse(self.repo2.check_overlaps())

class NumbexDBMergeTestInconsistent(NumbexDBMergeTestBase):
    def test_merge_inconsistent(self):
        # make a situation like this
        # self:   111 333
        # remote: 2222222
        # where 111 is the oldest record and 333 is the newest
        self.assertEqual(self.repo1.get_range('+481000'), self.data[0])
        self.assert_(self.repo1.import_data([self.record6], delete=['+482500']))
        self.repo1.sync()
        self.assertEqual(self.repo1.get_range('+481000'), self.data[0])
        self.assertEqual(self.repo1.get_range('+482000'), self.record6)
        self.assert_(self.repo2.import_data([self.record4], delete=['+482500']))
        self.repo2.sync()
        self.repo2.fetch_from_remote('repo1')
        self.assertRaises(gitdb.NumbexDBError,
                self.repo2.merge, 'repo1/'+self.repo1.repobranch)

class NumbexDBMergeTestFixup(NumbexDBMergeTestBase):
    def test_merge(self):
        # self:   111 222
        # remote: 3333333     result => 3333333
        r1 = ['+48100000',
              '+48100099',
              'sip.freeconet.pl',
              'freeconet',
              datetime.datetime(2009, 2, 9, 11, 0),
              'AAAAFGgYIQjyImbwnuMNX42U4NbgfmHZ AAAAFES+AFj7QgWpv/wz8NC7BGfahWPU']
        r2 = ['+48100100',
              '+48100199',
              'new.freeconet.pl',
              'freeconet',
              datetime.datetime(2009, 2, 10, 11, 0),
              'AAAAFEd3kvSFSDSWRtDPirvdDlO894ta AAAAFAz3sbHl0yzPrsgEackyZ5mkALNu']
        r3 = ['+48100000',
              '+48100299',
              'sip.freeconet.pl',
              'freeconet',
              datetime.datetime(2009, 2, 11, 11, 0),
              'AAAAFQCI0SZoDnaVDYPtmMgFEr2wrzkOFQ== AAAAFEQyWX/+Bolov/VFaHWweRUnJbYp']

        self.repo1.import_data([r1, r2])
        self.repo1.sync()
        self.repo2.import_data([r3])
        self.repo2.sync()
        self.repo2.fetch_from_remote('repo1')
        self.repo2.merge('repo1/'+self.repo1.repobranch)
        self.repo2.reload()
        self.assertFalse(self.repo2.check_overlaps())


class NumbexDBRevertTest(NumbexDBMergeTestBase):
    def test_merge(self):
        self.repo1.import_data([self.record1])
        self.repo1.sync()
        self.repo2.import_data([self.record2, self.record3])
        self.repo2.sync()
        self.repo2.fetch_from_remote('repo1')
        self.repo2.merge('repo1/'+self.repo1.repobranch)
        self.repo2.reload()
        self.repo2.sync()
        self.assertEqual(self.repo2.get_range('+484000'), self.record2)
        self.assertEqual(self.repo2.get_range('+485000'), self.record3)
        self.assertEqual(self.repo2.get_range('+481000'), self.data[0])
        delete = ['+481000', '+484000', '+485000']
        self.assertFalse(self.repo2.import_data([self.record4], delete))
        delete += ['+482500']
        self.assert_(self.repo2.import_data([self.record4], delete))
        expected = [self.record4]
        self.assertEqual(self.repo2.export_data_all(), expected)
        self.repo1.fetch_from_remote('repo2')
        self.repo1.merge('repo2/'+self.repo2.repobranch)
        self.repo1.reload()
        self.repo1.sync()
        self.assertEqual(self.repo1.export_data_all(), expected)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main()

