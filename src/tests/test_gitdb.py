from __future__ import absolute_import
import unittest
import gitdb

class NumbexDBPrimitiveTest(unittest.TestCase):
    def setUp(self):
        self.repo = gitdb.NumbexRepo('')

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


if __name__ == '__main__':
    unittest.main()
