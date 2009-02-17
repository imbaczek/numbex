#!/usr/bin/python

import logging, logging.config
import time
import datetime
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
        self.p2p_running = False
        self.log = logging.getLogger("daemon")

    def reload_config(self, configname):
        newcfg = read_config(confname)
        # TODO do stuff that needs to be done after config changes
        self.cfg = newcfg

    def p2p_get_peers(self):
        s = set()
        for c in self.clients:
            for p in c.peers:
                s.add(p)
        return list(s)
        
    def p2p_get_updates(self):
        pass

    def p2p_stop(self):
        if self.p2p_running:
            self.log.info("stopping p2p")
        # even if flag not set, cleaning up can't hurt
        for p in self.clients:
            p.stop()
        self.clients = []
        self.p2p_running = False

    def p2p_start(self):
        if self.p2p_running:
            self.log.info("p2p already running")
            return
        self.log.info("starting p2p peers")
        user = self.cfg.get('PEER', 'user')
        auth = self.cfg.get('PEER', 'auth')
        trackers = self.cfg.get('PEER', 'trackers').split()
        for track in trackers:
            peer = NumbexPeer(user=user, auth=auth, address=track,
                git=self.git, gitaddress=self.cfg.get('GIT', 'repo_url'))
            self.clients.append(peer)
            peer.register()
            t = threading.Thread(target=peer.mainloop)
            t.daemon = True
            t.start()
        self.p2p_running = True

    def import_from_p2p(self, force_all=False):
        if force_all or self.db.ranges_empty():
            if force_all:
                self.log.info("forced import")
            else:
                self.log.info("database empty, importing all...")
            start = time.time()   
            r = self.db.update_data(self.git.export_data_all())
            end = time.time()
            if r:
                self.log.info("database import completed in %.3f", end-start)
                return True, ""
            else:
                self.log.warn("database import failed, time %.3f", end-start)
                return False, "update_data returned False"

        if self.db.has_changed_data():
            return False, "database has changed data"

        hours = self.cfg.getint('GIT', 'export_timeout')
        since = datetime.datetime.now() - datetime.timedelta(hours)
        self.log.info("importing records modified since %s into the database",
                since)
        start = time.time()
        r = self.db.update_data(self.git.export_data_since(since))
        end = time.time()
        if r:
            self.log.info("database import completed in %.3f", end-start)
            return True, ""
        else:
            self.log.info("database import failed, time %.3f", end-start)
            return False, "update_data returned False"

    def export_to_p2p(self):
        if not self.db.has_changed_data():
            return True
        if self.git:
            hours = self.cfg.getint('DATABASE', 'export_timeout')
            since = datetime.datetime.now() - datetime.timedelta(hours)
            self.log.info("importing records since %s into git", since)
            start = time.time()
            r = self.git.import_data(self.db.get_data_since(since))
            end = time.time()
        else:
            self.log.info("git repo empty, importing all records...")
            start = time.time()
            r = self.git.import_data(self.db.get_data_all())
            end = time.time()
        if r:
            self.log.info("import complete, time %.3f", end-start)
        else:
            self.log.warn("import failed, time %.3f", end-start)
        return r
        

    def shutdown(self):
        self._exit()

    def _startup(self):
        gitpath = os.path.expanduser(self.cfg.get('GIT', 'path'))
        self.db = Database(os.path.expanduser(self.cfg.get('DATABASE', 'path')))
        self.git = NumbexRepo(os.path.expanduser(self.cfg.get('GIT', 'path')),
                self.db.get_public_keys)
        if self.db.ranges_empty():
            self.log.info("initial database empty")
            self.import_from_p2p()
        elif not self.git:
            self.log.info("initial git repository empty")
            self.export_to_p2p()
        self.git.start_daemon(self.cfg.getint('GIT', 'daemon_port'))
        self.p2p_start()
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
            pass
        except:
            self.log.exception("exception:")
            self._stop()
            sys.exit(2)
        finally:
            self._exit()

    def _exit(self, sig=None, frame=None):
        if sig is not None:
            self.log.info("received signal %s", sig)
        self._stop()
        sys.exit(0)

    def _stop(self):
        self.p2p_stop()
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
