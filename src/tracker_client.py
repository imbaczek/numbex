#!/usr/bin/python

import xmlrpclib
import socket
import threading
import time
import logging
import os, sys

class NumbexPeer(object):
    def __init__(self, address='http://localhost:8880', user=None, auth=None,
                git=None, gitaddress=None):
        if gitaddress is None:
            raise ValueError('gitaddress cannot be None')
        self.address = address
        self.user = user
        self.auth = auth
        self.git = git
        self.rpc = xmlrpclib.ServerProxy(address)
        self.timeout = 300
        self.running = False
        self.gitaddress = gitaddress
        self.peers = []
        self.firstupdate = True
        self.reregister = False

    def keepalive(self):
        while self.running and self.timeout > 0:
            try:
                known = self.rpc.keepalive(self.gitaddress)
                if not known:
                    logging.warn("server forgot about us, need to reregister")
                    self.reregister = True
                    return
            except (socket.error, socket.timeout), e:
                logging.error("keepalive: %s", e)
            time.sleep(max(self.timeout-5, 1))

    def register(self):
        while True:
            try:
                self.timeout = self.rpc.register(self.user, self.auth,
                        self.gitaddress)
            except (socket.error, socket.timeout):
                logging.exception("error while registering")
                self.timeout = 0
            if self.timeout > 0:
                logging.info("registered, timeout period %s", self.timeout)
                self.running = True
                self.reregister = False
                t = threading.Thread(target=self.keepalive)
                t.daemon = True
                t.start()
                return
            else:
                logging.warn("cannot register, retrying...")
                time.sleep(20)

    def stop(self):
        self.running = False
        try:
            self.rpc.unregister(self.gitaddress)
        except:
            logging.exception("unregister failed")

    def get_peers(self):
        self.peers = self.rpc.get_peers(self.gitaddress)
        logging.info("got %s peers", len(self.peers))

    def mainloop(self):
        while self.running:
            try:
                # sleep unless it's our first update
                if not self.firstupdate:
                    time.sleep(self.timeout)
                else:
                    self.firstupdate = False
                # check if we have to reregsiter
                if self.reregister:
                    self.stop()
                    self.register()
                self.get_peers()
            except (KeyboardInterrupt, SystemExit):
                logging.info("exiting")
                self.stop()
                return
            except (socket.error, socket.timeout), e:
                logging.error("%s", e)
            except:
                logging.exception('caught exception')
        self.stop()



def main():
    logging.basicConfig(level=logging.INFO)
    myaddr = socket.gethostbyname(socket.getfqdn())
    peer = NumbexPeer(user='test', auth='test')
    peer.register()
    peer.mainloop()
    sys.exit(0)

if __name__ == '__main__':
    main()

