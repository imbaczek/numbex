#!/usr/bin/python

import logging, logging.config
import time
import SocketServer
import fcntl
import traceback
import xmlrpclib
import sys
import threading
import os
import signal
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
from ConfigParser import SafeConfigParser as ConfigParser
from SimpleXMLRPCServer import SimpleXMLRPCServer, SimpleXMLRPCDispatcher, \
        SimpleXMLRPCRequestHandler, Fault


from tracker_client import NumbexPeer
from gitdb import NumbexRepo
from database import Database


def read_config(confname):
    from defaultconf import defaults
    cfg = ConfigParser()
    sio = StringIO(defaults)
    cfg.readfp(sio, 'default')
    cfg.read(confname)
    return cfg


class NumbexDaemon(object):
    def __init__(self, cfg):
        self.cfg = cfg
        self.clients = []
        self.git = None
        self.db = None

    def reload_config(self, configname):
        newcfg = read_config(confname)
        # TODO do stuff that needs to be done after config changes
        self.cfg = newcfg

    def p2p_get_updates(self):
        pass

    def p2p_stop(self):
        pass

    def p2p_start(self):
        pass

    def import_from_p2p(self):
        pass

    def export_to_p2p(self):
        pass

    def shutdown(self):
        self._exit()

    def _startup(self):
        trackers = self.cfg.get('PEER', 'trackers').split()
        user = self.cfg.get('PEER', 'user')
        auth = self.cfg.get('PEER', 'auth')
        gitpath = os.path.expanduser(self.cfg.get('GIT', 'path'))
        self.db = Database(os.path.expanduser(self.cfg.get('DATABASE', 'path')))
        self.git = NumbexRepo(os.path.expanduser(self.cfg.get('GIT', 'path')),
                self.db.get_public_keys)
        for track in trackers:
            peer = NumbexPeer(user=user, auth=auth, address=track,
                git=self.git, gitaddress=self.cfg.get('GIT', 'repo_url'))
            self.clients.append(peer)
            peer.register()
            t = threading.Thread(target=peer.mainloop)
            t.daemon = True
            t.start()
        self.git.start_daemon(self.cfg.getint('GIT', 'daemon_port'))
        signal.signal(signal.SIGTERM, self._exit)

    def _main(self):
        try:
            self._startup()
        except:
            self._stop()
            raise
        try:
            host = self.cfg.get('GLOBAL', 'control_host')
            port = self.cfg.getint('GLOBAL', 'control_port')
            xmlrpc = SimpleXMLRPCServer((host, port))
            xmlrpc.register_instance(self)
            xmlrpc.serve_forever()
        except (KeyboardInterrupt, SystemExit):
            self._exit()
        finally:
            self._stop()

    def _exit(self, sig=None, frame=None):
        if sig is not None:
            logging.info("received signal %s", sig)
        self._stop()
        sys.exit(0)

    def _stop(self):
        for p in self.clients:
            p.stop()
        self.clients = []
        if self.git is not None:
            self.git.dispose()
            self.git = None
        self.db = None


def main():
    from optparse import OptionParser
    op = OptionParser(usage="%prog [options]")
    op.add_option("-C", "--config-file", help="config file to use",
        metavar="CONFIG_FILE", default="/etc/numbex.conf")
    op.add_option("-L", "--log-config-file", help="logging config file to use",
        metavar="LOG_CONF_FILE", default="")
    options, args = op.parse_args()

    logging.basicConfig(level=logging.INFO)
    cfg = read_config(options.config_file)
    if cfg.get('GLOBAL', 'logging_config'):
        logging.config.fileConfig(cfg.get('GLOBAL', 'logging_config'))
    if options.log_config_file:
        logging.config.fileConfig(options.log_config_file)
    daemon = NumbexDaemon(cfg)
    daemon._main()



if __name__ == '__main__':
    main()
