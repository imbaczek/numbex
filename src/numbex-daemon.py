#!/usr/bin/python

import logging, logging.config
import time
import datetime
import xmlrpclib
import sys
import threading
import os
import signal
import random
import re
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
from ConfigParser import SafeConfigParser as ConfigParser
from SimpleXMLRPCServer import SimpleXMLRPCServer, SimpleXMLRPCDispatcher, \
        SimpleXMLRPCRequestHandler, Fault
from Queue import Queue

from ZSI.ServiceContainer import AsServer

from numbex_server import MyNumbexService
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
        self.updater_running = False
        self.updater_sched_stopped = True
        self.updater_worker_stopped = True
        self.updater_reqs = Queue(20)
        self.last_update = 0

    def reload_config(self, configname):
        newcfg = read_config(confname)
        # TODO do stuff that needs to be done after config changes
        self.cfg = newcfg

    def _p2p_get_peer_set(self):
        s = set()
        for c in self.clients:
            for p in c.peers:
                s.add(p)
        return s

    def p2p_get_peers(self):
        s = self._p2p_get_peer_set()
        return list(s)
 
    def p2p_get_updates(self, requested=None):
        try:
            self.updater_reqs.put_nowait(requested)
            return True
        except:
            return False

    def _p2p_get_updates(self, requested=None, git=None):
        if git is None:
            git = self.git
        # if no peers specificalyl requested, pick a random one
        if requested is None:
            self.log.info("get updates from random peer...")
            peers = self.p2p_get_peers()
            if peers:
                requested = [random.choice(peers)]
            else:
                return False, "no known peers"
        else:
            self.log.info("get updates")
            peers = self._p2p_get_peer_set()
            for p in requested:
                if p not in peers:
                    return False, "%s is an unknown peer"%p
        for p in requested:
            self.log.info("fetching from %s...", p)
            start = time.time()
            remote = re.sub(r'[^a-zA-Z0-9_-]', '', 'remote_%s' % p)
            git.add_remote(remote, p, force=True)
            git.fetch_from_remote(remote)
            git.merge('%s/%s'%(remote, 'numbex'))
            git.reload()
            end = time.time()
            self.log.info("fetch and merge complete in %.3fs", end-start)
        return True, ""

    def _updater_thread(self):
        self.log.info("starting update processor")
        # need our own database connection here
        # and our own git, too, since it needs public keys
        db = Database(os.path.expanduser(self.cfg.get('DATABASE', 'path')))
        git = NumbexRepo(os.path.expanduser(self.cfg.get('GIT', 'path')),
                db.get_public_keys)
        self.updater_worker_stopped = False
        while self.updater_running:
            requested = self.updater_reqs.get()
            if not self.updater_running:
                break
            if [None] == requested:
                continue
            try:
                r, msg = self._p2p_get_updates(requested, git=git)
                if not r:
                    self.log.warn("p2p_get_updates: %s", msg)
                if not self.updater_running:
                    break
                self._import_from_p2p(db=db)
                self.last_update = time.time()
            except:
                self.log.exception("error in p2p_get_updates")
        self.updater_worker_stopped = True
        self.log.info("update processor stopped")

    def _updater_scheduler_thread(self):
        ival = self.cfg.getint('PEER', 'fetch_interval') 
        self.log.info("starting update scheduler, interval %s", ival)
        self.updater_sched_stopped = False
        while self.updater_running:
            ival = self.cfg.getint('PEER', 'fetch_interval') 
            time.sleep(ival)
            if not self.updater_running:
                break
            self.updater_reqs.put(None)
        self.updater_sched_stopped = True
        self.updater_reqs.put([None])
        self.log.info("update scheduler stopped")


    def updater_start(self):
        if self.updater_running:
            return False
        if not self.updater_sched_stopped or not self.updater_worker_stopped:
            self.log.info("updater threads not stopped yet")
            return False
        if self.cfg.getint('PEER', 'fetch_interval') <= 0:
            self.log.warn("updater disabled due to intveral=%s",
                    self.cfg.getint('PEER', 'fetch_interval'))
            return False
        self.updater_running = True
        t = threading.Thread(target=self._updater_scheduler_thread)
        t.daemon = True
        t.start()
        t = threading.Thread(target=self._updater_thread)
        t.daemon = True
        t.start()
        return True

    def updater_stop(self):
        self.log.info("stopping updater threads")
        self.updater_running = False
        return True

    def p2p_stop(self):
        self.git.stop_daemon()
        if self.p2p_running:
            self.log.info("stopping p2p")
        # even if flag not set, cleaning up can't hurt
        for p in self.clients:
            p.stop()
        self.clients = []
        self.p2p_running = False
        return True

    def p2p_start(self):
        if self.p2p_running:
            self.log.info("p2p already running")
            return False
        self.git.start_daemon(self.cfg.getint('GIT', 'daemon_port'))
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
        return True

    def _import_from_p2p(self, db, force_all=False):
        if force_all or db.ranges_empty():
            if force_all:
                self.log.info("forced import of everything")
            else:
                self.log.info("database empty, importing all...")
            start = time.time()   
            r = db.update_data(self.git.export_data_all())
            end = time.time()
            if r:
                self.log.info("database import completed in %.3f", end-start)
                return True, ""
            else:
                self.log.warn("database import failed, time %.3f", end-start)
                return False, "update_data returned False"

        if db.has_changed_data():
            return False, "database has changed data"

        hours = self.cfg.getint('GIT', 'export_timeout')
        since = datetime.datetime.now() - datetime.timedelta(hours)
        self.log.info("importing records modified since %s into the database",
                since)
        start = time.time()
        r = db.update_data(self.git.export_data_since(since))
        end = time.time()
        if r:
            self.log.info("database import completed in %.3f", end-start)
            return True, ""
        else:
            self.log.info("database import failed, time %.3f", end-start)
            return False, "update_data returned False"

    def import_from_p2p(self, force_all=False):
        return self._import_from_p2p(self.db, force_all)

    def export_to_p2p(self):
        if not self.db.has_changed_data():
            return True
        try:
            if self.git:
                hours = self.cfg.getint('DATABASE', 'export_timeout')
                since = datetime.datetime.now() - datetime.timedelta(hours)
                self.log.info("importing records since %s into git", since)
                start = time.time()
                delete = [x[0] for x in self.db.get_deleted_data()]
                r = self.git.import_data(self.db.get_data_since(since),
                        delete=delete)
                end = time.time()
            else:
                self.log.info("git repo empty, importing all records...")
                start = time.time()
                r = self.git.import_data(self.db.get_data_all())
                end = time.time()
        except:
            self.log.exception("export_to_p2p")
            raise
        if r:
            self.log.info("import complete, time %.3f", end-start)
            self.db.clear_changed_data()
        else:
            self.log.warn("import failed, time %.3f", end-start)
        return r

    def status(self):
        return {
            'flags': {
                'p2p_running': self.p2p_running,
                'updater_running': self.updater_running,
            },
            'p2p': {
                'peers': self.p2p_get_peers(),
                'trackers': [x.address for x in self.clients],
            },
            'updater': {
                'last_update': self.last_update,
            },
            'database': {
                'has_changed_data': self.db.has_changed_data(),
            }
        }

    def shutdown(self):
        self._exit()

    def _soap_start(self):
        port = self.cfg.getint('SOAP', 'port')
        self.log.info("starting SOAP controller on port %s...", port)
        dbpath = os.path.expanduser(self.cfg.get('DATABASE', 'path'))
        t = threading.Thread(target=AsServer,
                kwargs=dict(port=port, services=[MyNumbexService(dbpath),]))
        t.daemon = True
        t.start()

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
        self.p2p_start()
        self.updater_start()
        self._soap_start()
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
            self.log.info("starting control daemon on %s port %s", host, port)
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
        try:
            self._stop()
        except:
            self.log.exception('exception during exit ignored:')
        sys.exit(0)

    def _stop(self):
        self.updater_stop()
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
