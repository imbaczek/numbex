#!/usr/bin/python
# *** ZSI 2.1 ***
from optparse import OptionParser
from datetime import datetime
import time
import logging
from cStringIO import StringIO
import csv

# Import the ZSI stuff you'd need no matter what
from ZSI.wstools import logging as zsilogging
from ZSI.ServiceContainer import AsServer

# Import the generated Server Object
from NumbexServiceService_server import NumbexServiceService

# database support
from database import Database
import crypto

# Create a Server implementation by inheritance
class MyNumbexService(NumbexServiceService):
    # Make WSDL available for HTTP GET
    _wsdl = file("NumbexServiceService.wsdl").read()
    def __init__(self, dbfile="tmp.db", *args, **kwargs):
        NumbexServiceService.__init__(self, *args, **kwargs)
        self.dbfile = dbfile
        self.db = None

    def _init_db(self):
        'deferred init of db to preserve threading constraints'
        if self.db is None:
            self.db = Database(self.dbfile)


    def _transform_to_csv(self, data):
        ofile = StringIO()
        csvwriter = csv.writer(ofile)
        for start, end, sip, owner, date, sig in data:
            csvwriter.writerow([start, end, sip, owner, date.isoformat(), sig])
        return ofile.getvalue()

    def soap_getData(self, ps, **kw):
        # Call the generated base class method to get appropriate
        # input/output data structures
        self._init_db()
        request, response = NumbexServiceService.soap_getData(self, ps, **kw)
        data = self._transform_to_csv(self.db.get_data_all())
        response._return = data
        return request, response

    def soap_getUpdates(self, ps, **kw):
        self._init_db()
        request, response = NumbexServiceService.soap_getUpdates(self, ps, **kw)
        since = datetime.fromtimestamp(time.mktime(request._parameter))
        response._return = self._transform_to_csv(self.db.get_data_since(since))
        return request, response

    def soap_receiveUpdates(self, ps, **kw):
        self._init_db()
        request, response = NumbexServiceService.soap_receiveUpdates(self, ps, **kw)
        data = request._csv
        # check validity of DSA signatures
        l = list(csv.reader(StringIO(data)))
        if not self.db.check_data_signatures(l):
            raise ValueError('signatures invalid')
        retval = self.db.update_data(l)
        response._return = retval
        return request, response

    def soap_getUnsigned(self, ps, **kw):
        self._init_db()
        request, response = NumbexServiceService.soap_getUnsigned(self, ps, **kw)
        response._return = self._transform_to_csv(self.db.get_data_unsigned())
        return request, response

    def soap_getPublicKeys(self, ps, **kw):
        self._init_db()
        request, response = NumbexServiceService.soap_getPublicKeys(self, ps, **kw)
        pubkeys = self.db.get_public_keys_ids(request._parameter)

        ret = ['id = %s\n%s'%(i, k) for i, k in pubkeys]
        response._return = '\n'.join(ret)
        return request, response

    def soap_removePublicKey(self, ps, **kw):
        self._init_db()
        request, response = NumbexServiceService.soap_removePublicKey(self, ps, **kw)
        self.db.remove_public_key(request._parameter)
        response._return = True
        return request, response

    def soap_receivePublicKey(self, ps, **kw):
        self._init_db()
        request, response = NumbexServiceService.soap_receivePublicKey(self, ps, **kw)
        owner = request._owner
        pubkeystr = request._pubkey
        pubkey = crypto.parse_pub_key(pubkeystr)
        if not pubkey.check_key():
            response._return = False
        else:
            response._return = True
            self.db.add_public_key(owner, pubkeystr)
        return request, response

def main():
    op = OptionParser(usage="%prog [options]")
    op.add_option("-l", "--loglevel", help="loglevel (DEBUG, WARN)",
        metavar="LOGLEVEL")
    op.add_option("-p", "--port", help="HTTP port",
        metavar="PORT", default=8000, type="int")
    op.add_option("-d", "--database", help="sqlite3 database filename",
        metavar="DBFILE", default="tmp.db")
    options, args = op.parse_args()
    # set the loglevel according to cmd line arg
    logging.basicConfig(level=logging.INFO)
    if options.loglevel:
        loglevel = eval(options.loglevel, zsilogging.__dict__)
        logger = zsilogging.getLogger("")
        logger.setLevel(zsiloglevel)
    # Run the server with a given list services
    AsServer(port=options.port, services=[MyNumbexService(options.database),])

if __name__ == '__main__':
    main()
