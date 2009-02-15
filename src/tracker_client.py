#!/usr/bin/python

import xmlrpclib
import socket
import threading
import time
import logging
import os, sys

class NumbexPeer(object):
    def __init__(self, address='http://localhost:8880', user=None, auth=None,
                git=None):
        self.address = address
        self.user = user
        self.auth = auth
        self.git = git
        self.rpc = xmlrpclib.ServerProxy(address)
        self.timeout = 300
        self.running = False
        self.gitaddress = ['127.0.0.1', os.getpid()]
        self.peers = []

    def keepalive(self):
        while self.running:
            try:
                self.rpc.keepalive(self.gitaddress)
            except socket.error, e:
                logging.error("%s", e)
            time.sleep(max(self.timeout-5, 1))

    def register(self):
        self.timeout = self.rpc.register(self.user, self.auth, self.gitaddress)
        if self.timeout != 0:
            logging.info("registering")
            self.running = True
            t = threading.Thread(target=self.keepalive)
            t.daemon = True
            t.start()

    def stop(self):
        self.running = False
        self.rpc.unregister(self.gitaddress)

    def get_peers(self):
        self.peers = self.rpc.get_peers(self.gitaddress)
        logging.info("got %s peers", len(self.peers))

    def mainloop(self):
        while self.running:
            try:
                self.get_peers()
                time.sleep(self.timeout)
            except (KeyboardInterrupt, SystemExit):
                logging.info("exiting")
                self.stop()
                return
            except socket.error, e:
                logging.error("%s", e)
            except:
                logging.exception('caught exception')



def main():
    logging.basicConfig(level=logging.INFO)
    myaddr = socket.gethostbyname(socket.getfqdn())
    peer = NumbexPeer(user='test', auth='test')
    peer.register()
    peer.mainloop()
    sys.exit(0)

if __name__ == '__main__':
    main()

