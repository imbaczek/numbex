import unittest
import threading
import signal
import os
import time

import database
import subprocess

from numbex_udp_server import serve_numbers_forever
from numbex_udp_client import query_server, UDPServerException


class ServerProcess(object):
    def __init__(self, port=58998):
        self.port = port

    def start(self):
        from random import random
        self.pid = os.fork()
        if not self.pid:
            os.execvp('./numbex_udp_server.py',
                    ('', '-p', str(self.port), '-b', 'localhost',
                     '/tmp/numbex_tmpdb.%s.%s.db'%(os.getpid(), random())))

    def kill(self):
        if self.pid:
            os.kill(self.pid, signal.SIGTERM)
            os.waitpid(self.pid, 0)



class TestUDPServer(unittest.TestCase):
    def setUp(self):
        self.port = 58998
        self.srv = ServerProcess(self.port)
        self.srv.start()
        # wait for the server to start
        time.sleep(1)

    def test_udp_200_OK(self):
        port = self.port
        expected = '+481234,+481299,sip.freeconet.pl,freeconet'
        # test edges of range
        result = query_server('+481234', 'localhost', port, timeout=5).rsplit(',', 2)[0]
        self.assertEqual(expected, result)
        result = query_server('+481299', 'localhost', port).rsplit(',', 2)[0]
        self.assertEqual(expected, result)
        # test inside of range
        for n in range(481235, 481299, 10):
            result = query_server('+'+str(n), 'localhost', port) \
                        .rsplit(',', 2)[0]
            self.assertEqual(expected, result)

    def test_udp_404(self):
        port = self.port
        self.assertEqual(None, query_server('+481300', 'localhost', port))
        self.assertEqual(None, query_server('+481199', 'localhost', port))
        self.assertEqual(None, query_server('+4810000', 'localhost', port))
        self.assertEqual(None, query_server('+1', 'localhost', port))

    def test_udp_500(self):
        port = self.port
        self.assertRaises(UDPServerException,
                query_server, 'tonienumer', 'localhost', port)
        self.assertRaises(UDPServerException,
                query_server, '+454654asfasf', 'localhost', port)
        self.assertRaises(UDPServerException,
                query_server, '+@!&#%&%$!@', 'localhost', port)

    def tearDown(self):
        self.srv.kill()
        # euthanize the server
        try:
            query_server('', 'localhost', 58998)
        except:
            pass
        


if __name__ == '__main__':
    unittest.main()
