#!/usr/bin/python

import xmlrpclib
import sys
from datetime import datetime

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
from ConfigParser import SafeConfigParser as ConfigParser


class NumbexDaemonController(object):
    def __init__(self, address):
        self.url = address
        self.rpc = xmlrpclib.ServerProxy(address)

    def export_to_p2p(self, options, args):
        r = self.rpc.export_to_p2p()
        if not r:
            sys.stderr.write('p2p-export failed, check signatures and logs\n')
        return r

    def import_from_p2p(self, options, args):
        r, msg = self.rpc.import_from_p2p()
        if not r:
            sys.stderr.write('''p2p-import failed: %s
check signatures and logs\n'''%msg)
        return r

    def p2p_start(self, options, args):
        r = self.rpc.p2p_start()
        if not r:
            sys.stderr.write('p2p-start failed, check logs\n')
        return r

    def p2p_stop(self, options, args):
        r = self.rpc.p2p_stop()
        if not r:
            sys.stderr.write('p2p-stop failed, check logs\n')
        return r

    def status(self, options, args):
        r = self.rpc.status()
        if not r:
            sys.stderr.write('status failed, check logs\n')
            return False
        print 'server status for %s:'%self.url
        flags = r['flags']
        for k in flags:
            print '%-20s: %s'%(k, flags[k])
        lastupdate = 'never' if not r['updater']['last_update'] else \
                datetime.fromtimestamp(r['updater']['last_update'])
        print '%-20s: %s'%('last git update', lastupdate)
        print
        print 'trackers:'
        for t in r['p2p']['trackers']:
            print '\t%s'%t
        print 'peers:'
        for p in r['p2p']['peers']:
            print '\t%s'%p
        print
        return True


def main():
    from optparse import OptionParser
    op = OptionParser(usage=""""%prog [options] <command>
    (use '%prog help' for available commands)""")
    op.add_option("-u", "--control-url", help="control url",
        metavar="LOG_CONF_FILE", default="http://localhost:44880")
    options, args = op.parse_args()

    ctl = NumbexDaemonController(options.control_url)
    def help(options, args):
        print """Available commands:

p2p-export\t\texport data to the p2p system from local database
p2p-import\t\timport data from the p2p system to the local database
p2p-start\t\tstart the p2p system, connect to trackers
p2p-stop\t\tdisconnect from trackers
status\t\tprint daemon status"""

    dispatch = {
        'help':       help,
        'p2p-export': ctl.export_to_p2p,
        'p2p-import': ctl.import_from_p2p,
        'p2p-start':  ctl.p2p_start,
        'p2p-stop':   ctl.p2p_stop,
        'status':     ctl.status,
    }

    if len(args) >= 1 and args[0] in dispatch:
        sys.exit(not dispatch[args[0]](options, args))
    else:
        op.error('invalid command')


if __name__ == '__main__':
    main()
