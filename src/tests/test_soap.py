import unittest
import threading
import signal
import os
import time
import datetime
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

import ZSI

import database
import crypto
import utils
from numbex_server import MyNumbexService
from numbex_client import pull, pull_all, pull_sign, send, send_sign


class ServerProcess(object):
    def __init__(self, port=58997):
        self.port = port

    def start(self):
        from random import random
        self.rnd = random()
        self.pid = os.fork()
        if not self.pid:
            self.dbname = '/tmp/numbex_tmpdb.%s.%s.db'%(os.getpid(), self.rnd)
            os.execvp('./numbex_server.py',
                    ('', '-p', str(self.port), '-d', self.dbname))
        else:
            self.dbname = '/tmp/numbex_tmpdb.%s.%s.db'%(self.pid, self.rnd)

    def kill(self):
        if self.pid:
            os.kill(self.pid, signal.SIGTERM)
            os.waitpid(self.pid, 0)
            try:
                os.unlink(self.dbname)
            except OSError:
                pass


class TestSOAPServer(unittest.TestCase):
    def setUp(self):
        self.port = 58997
        self.srv = ServerProcess(self.port)
        self.srv.start()
        self.url = 'http://localhost:%s/'%self.port
        # wait for the server to start
        time.sleep(1)

    def test_soap(self):
        port = self.port
        pemkeys = database.Database._example_privkeys
        extract_pubkey = lambda x: crypto.extract_pub_key(
                crypto.parse_priv_key(pemkeys[x]))
        keys = dict(freeconet=extract_pubkey('freeconet'),
                telarena=extract_pubkey('telarena'))
        basedatacsv = '''+481234,+481299,sip.freeconet.pl,freeconet,2009-01-03T23:59:30.123456
+4820000,+4820999,sip.freeconet.pl,freeconet,2009-01-03T23:59:30.123456
+4821000,+4821111,sip.freeconet.pl,freeconet,2009-01-08T23:59:30.123456
+4830000,+4830099,sip.freeconet.pl,freeconet,2009-01-08T23:59:30.123456'''
        basedata = utils.parse_csv_data(StringIO(basedatacsv))

        tosend = '''+4821005,+4821011,new.freeconet.pl,freeconet,2009-01-09T20:11:58.312000
+481222,+481240,new.freeconet.pl,freeconet,2009-01-09T20:11:58.312000
+481298,+481301,new.freeconet.pl,freeconet,2009-01-09T20:11:58.312000
+4821113,+4821123,new.freeconet.pl,freeconet,2009-01-09T20:11:58.312000'''
        # no signatures
        self.assertRaises(ZSI.FaultException, send, StringIO(tosend), url=self.url)
        # test overwriting
        send_sign(StringIO(basedatacsv), StringIO(pemkeys['freeconet']), url=self.url)
        result = pull_all(url=self.url)
        parsed = utils.parse_csv_data(StringIO(result))
        for rowe, rowr in zip(basedata, parsed):
            self.assertEqual(rowr[:-1], rowe)
        for row in parsed:
            key = crypto.parse_pub_key(keys[row[3]])
            self.assert_(crypto.check_signature(key, row[5], *row))

        # test updates
        send_sign(StringIO(tosend), StringIO(pemkeys['freeconet']), url=self.url)
        expected = utils.parse_csv_data(StringIO('''+481241,+481297,sip.freeconet.pl,freeconet,2009-01-08T23:59:30.123456
+4821000,+4821004,sip.freeconet.pl,freeconet,2009-01-08T23:59:30.123456
+4821012,+4821111,sip.freeconet.pl,freeconet,2009-01-08T23:59:30.123456'''))
        result = pull_sign(url=self.url)
        parsed = utils.parse_csv_data(StringIO(result))
        for rowe, rowr in zip(expected, parsed):
            self.assertEqual(rowe[:-1], rowr[:-2])
 

    def tearDown(self):
        self.srv.kill()
        # euthanize the server
        try:
            pull(datetime.datetime.now(), url=self.url)
        except:
            pass


if __name__ == '__main__':
    unittest.main()
