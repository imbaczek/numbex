from optparse import OptionParser
import sys, time
from datetime import datetime

from NumbexServiceService_client import *

_outfile = sys.stdout
_tracefile = None
_url = 'http://localhost:8000/'

def pull_all():
    loc = NumbexServiceServiceLocator()
    port = loc.getNumbexServicePort(url=_url, tracefile=_tracefile)

    msg = getData()
    rsp = port.getData(msg)
    _outfile.write(rsp._return)


def pull(since):
    loc = NumbexServiceServiceLocator()
    port = loc.getNumbexServicePort(url=_url, tracefile=_tracefile)

    msg = getUpdates()
    s = datetime.strptime(since, "%Y-%m-%dT%H:%M:%S")
    msg._parameter = s.utctimetuple()
    rsp = port.getUpdates(msg)
    _outfile.write(rsp._return)


def send(filename):
    loc = NumbexServiceServiceLocator()
    port = loc.getNumbexServicePort(url=_url, tracefile=_tracefile)

    msg = receiveUpdates()
    data = file(filename).read()
    msg._csv = data
    rsp = port.receiveUpdates(msg)

def main():
    global _tracefile, _outfile, _url
    usage = """%prog [options] <command>\n
Available commands:
    pull <DATE_SINCE>\tget numer ranges modified after DATE_SINCE
    pullall          \tget all number ranges
    send <CSVFILE>   \tsend the contents of CSVFILE as an update"""
    op = OptionParser(usage=usage)
    op.add_option("-o", "--output-file", help="output file",
            metavar="OUTFILE")
    op.add_option("-t", "--trace-file", help="trace file (for debugging)",
            metavar="TRACEFILE")
    op.add_option("-u", "--url", help="url of the webservice",
            metavar="URL", default=_url)	
    options, args = op.parse_args()
    _tracefile = options.trace_file
    if options.output_file:
        _outfile = file(options.output_file, "wb")
    if len(args) < 1:
        op.error("command required")
        return
    _url = options.url
    dispatch = {'pullall': pull_all,
                'pull': pull,
                'send': send}
    try:
        dispatch.get(args[0], lambda: op.error("unknown command"))(*args[1:])
    except TypeError:
        op.error("invalid parameters")
        return

if __name__ == "__main__":
    main()
