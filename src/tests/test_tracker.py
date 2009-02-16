import unittest
import threading
import signal
import os
import time

from tracker_client import NumbexPeer

class TrackerServerProcess(object):
    def __init__(self, port=58996):
        self.port = port

    def start(self):
        from random import random
        self.pid = os.fork()
        if not self.pid:
            os.execvp('./tracker.py',
                    ('', '-p', str(self.port), '-b', 'localhost', '-t', '2'))

    def kill(self):
        if self.pid:
            os.kill(self.pid, signal.SIGTERM)
            os.waitpid(self.pid, 0)



class TestUDPServer(unittest.TestCase):
    def setUp(self):
        self.port = 58998
        self.srv = TrackerServerProcess(self.port)
        self.srv.start()
        self.url = 'http://localhost:%s'%self.port
        # wait for the server to start
        time.sleep(1)

    def test_peers(self):
        p1 = NumbexPeer(user='test', auth='test',
                address=self.url, gitaddress=('127.0.0.1', 11111))
        p2 = NumbexPeer(user='test', auth='test',
                address=self.url, gitaddress=('127.0.0.1', 22222))
        p1.register()
        p2.register()
        t1 = threading.Thread(target=p1.mainloop)
        t2 = threading.Thread(target=p2.mainloop)
        t1.daemon = True
        t1.start()
        t2.daemon = True
        t2.start()
        time.sleep(2)
        self.assertEqual(p1.peers, [['127.0.0.1', 22222]])
        self.assertEqual(p2.peers, [['127.0.0.1', 11111]])
        p1.stop()
        p2.stop()
        time.sleep(1)

    def tearDown(self):
        self.srv.kill()


if __name__ == '__main__':
    unittest.main()
