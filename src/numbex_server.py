#!/apps/pydev/hjoukl/bin/python2.4
# *** ZSI 2.1 ***
from optparse import OptionParser
# Import the ZSI stuff you'd need no matter what
from ZSI.wstools import logging
from ZSI.ServiceContainer import AsServer

# Import the generated Server Object

from NumbexServiceService_server import NumbexServiceService

# Create a Server implementation by inheritance
class MyNumbexService(NumbexServiceService):
    # Make WSDL available for HTTP GET
    _wsdl = file("NumbexServiceService.wsdl").read()
    def soap_getData(self, ps, **kw):
        # Call the generated base class method to get appropriate
        # input/output data structures
        request, response = NumbexServiceService.soap_getData(self, ps, **kw)
        response._return = self.getSquare(1)
        return request, response

    def getSquare(self, x):
        return "1234fsadfasf"


op = OptionParser(usage="%prog [options]")
op.add_option("-l", "--loglevel", help="loglevel (DEBUG, WARN)",
metavar="LOGLEVEL")
op.add_option("-p", "--port", help="HTTP port",
metavar="PORT", default=8000, type="int")
options, args = op.parse_args()
# set the loglevel according to cmd line arg
if options.loglevel:
    loglevel = eval(options.loglevel, logging.__dict__)
    logger = logging.getLogger("")
    logger.setLevel(loglevel)
# Run the server with a given list services
AsServer(port=options.port, services=[MyNumbexService(),])

