import unittest
import threading
import time

from btpeer import BTPeer, BTPeerConnection


class BTPeerTest(unittest.TestCase):
    def setUp(self):
        self.peer1 = BTPeer(5, 15151)
        self.peer1.debug = True
        self.peer2 = BTPeer(5, 25252)
        self.peer2.debug = True
        self.t1 = threading.Thread(target = self.peer1.mainloop)
        self.t2 = threading.Thread(target = self.peer2.mainloop)

    def test_simple(self):
        m1 = 'hello from peer2'
        m2 = 'HELLO FROM PEER1'
        returns = {}
        def handler1(peerconn, msgdata, returns=returns):
            print peerconn, msgdata
            returns['p1'] = msgdata
        def handler2(peerconn, msgdata, returns=returns):
            print peerconn, msgdata
            returns['p2'] = msgdata

        self.peer1.addhandler('HELL', handler1)
        self.peer2.addhandler('HELL', handler2)

        self.t1.start()
        self.t2.start()

        time.sleep(0.5)

        self.peer1.connectandsend('localhost', 25252, 'HELL', m2)
        self.peer2.connectandsend('localhost', 15151, 'HELL', m1)

        time.sleep(0.1)

        self.assertEqual(returns['p1'], m1)
        self.assertEqual(returns['p2'], m2)

    def tearDown(self):
        self.peer1.shutdown = True
        self.peer2.shutdown = True
        # motivate threads to exit
        try:
            c = BTPeerConnection(None, 'localhost', 15151)
            c.close()
        except socket.error:
            pass
        try:
            c = BTPeerConnection(None, 'localhost', 25252)
            c.close()
        except socket.error:
            pass


if __name__ == '__main__':
    unittest.main()
