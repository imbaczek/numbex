#!/usr/bin/python
# *** ZSI 2.1 ***
from optparse import OptionParser
from datetime import datetime
import time
from cStringIO import StringIO
import csv

# Import the ZSI stuff you'd need no matter what
from ZSI.wstools import logging
from ZSI.ServiceContainer import AsServer

# Import the generated Server Object
from NumbexServiceService_server import NumbexServiceService

# database support
from database import Database

# Create a Server implementation by inheritance
class MyNumbexService(NumbexServiceService):
    # Make WSDL available for HTTP GET
    _wsdl = file("NumbexServiceService.wsdl").read()
    def __init__(self, dbfile="tmp.db", *args, **kwargs):
        NumbexServiceService.__init__(self, *args, **kwargs)
        self.db = Database(dbfile)

    def _transform_to_csv(self, data):
        ofile = StringIO()
        csvwriter = csv.writer(ofile)
        for start, end, sip, owner, date, sig in data:
            csvwriter.writerow([start, end, sip, owner, date.isoformat(), sig])
        return ofile.getvalue()

    def soap_getData(self, ps, **kw):
        # Call the generated base class method to get appropriate
        # input/output data structures
        request, response = NumbexServiceService.soap_getData(self, ps, **kw)
        data = self._transform_to_csv(self.db.get_data_all())
        response._return = data
        return request, response

    def soap_getUpdates(self, ps, **kw):
        request, response = NumbexServiceService.soap_getUpdates(self, ps, **kw)
        since = datetime.fromtimestamp(time.mktime(request._parameter))
        response._return = self._transform_to_csv(self.db.get_data_since(since))
        return request, response

    def soap_receiveUpdates(self, ps, **kw):
        request, response = NumbexServiceService.soap_receiveUpdates(self, ps, **kw)
        data = request._csv
        l = list(csv.reader(StringIO(data)))
        self.db.update_data(l)
        response._return = True
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
    if options.loglevel:
        loglevel = eval(options.loglevel, logging.__dict__)
        logger = logging.getLogger("")
        logger.setLevel(loglevel)
    # Run the server with a given list services
    AsServer(port=options.port, services=[MyNumbexService(options.database),])

if __name__ == '__main__':
    main()
