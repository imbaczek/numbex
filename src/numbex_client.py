#!/usr/bin/python
from optparse import OptionParser
import sys, time
import csv
import logging
from datetime import datetime

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO


from NumbexServiceService_client import *
import crypto
import utils

_outfile = sys.stdout
_tracefile = None
_url = 'http://localhost:8000/'

def pull_all(url=None):
    if url is None:
        url = _url
    loc = NumbexServiceServiceLocator()
    port = loc.getNumbexServicePort(url=_url, tracefile=_tracefile)

    msg = getData()
    rsp = port.getData(msg)
    return rsp._return


def pull(since, url=None):
    if url is None:
        url = _url
    loc = NumbexServiceServiceLocator()
    port = loc.getNumbexServicePort(url=url, tracefile=_tracefile)

    msg = getUpdates()
    s = datetime.strptime(since, "%Y-%m-%dT%H:%M:%S")
    msg._parameter = s.timetuple()
    rsp = port.getUpdates(msg)
    return rsp._return

def pull_sign(url=None):
    if url is None:
        url = _url
    loc = NumbexServiceServiceLocator()
    port = loc.getNumbexServicePort(url=url, tracefile=_tracefile)

    msg = getUnsigned()
    rsp = port.getUnsigned(msg)
    return rsp._return

def send(filename, url=None):
    if url is None:
        url = _url
    loc = NumbexServiceServiceLocator()
    port = loc.getNumbexServicePort(url=url, tracefile=_tracefile)

    msg = receiveUpdates()
    data = file(filename).read()
    msg._csv = data
    rsp = port.receiveUpdates(msg)
    if not rsp._return:
        logging.error("send: receiveUpdates() failed, check input data")
        sys.exit(1)


def send_sign(csvfile, keyfile, url=None):
    if url is None:
        url = _url
    try:
        dsa = crypto.parse_priv_key(keyfile.read())
    except AttributeError:
        dsa = crypto.parse_priv_key(file(keyfile).read())
    loc = NumbexServiceServiceLocator()
    port = loc.getNumbexServicePort(url=url, tracefile=_tracefile)

    msg = receiveUpdates()
    reader = csv.reader(file(csvfile))
    sio = StringIO()
    writer = csv.writer(sio)

    for row in reader:
        if len(row) < 5:
            logging.error("invalid record: %s", row)
            sys.exit(1)
        row[4] = utils.parse_datetime_iso(row[4])
        sig = crypto.sign_record(dsa, *row[:5])
        writer.writerow((row[0], row[1], row[2], row[3], row[4].isoformat(), sig))

    msg._csv = sio.getvalue()
    rsp = port.receiveUpdates(msg)
    if not rsp._return:
        logging.error("send: receiveUpdates() failed, check input data")
        sys.exit(1)

def main():
    global _tracefile, _outfile, _url
    usage = """%prog [options] <command>\n
Available commands:
    pull <DATE_SINCE>\tget numer ranges modified after DATE_SINCE
    pullall          \tget all number ranges
    pullsign         \tget all unsigned ranges
    sendsign <CSVFILE> <KEYFILE>
                     \tsend the contents of CSVFILE as an update, sign them
                     \twith KEYFILE (PEM private key)
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
                'pullsign': pull_sign,
                'send': send,
                'sendsign': send_sign}
    try:
        r=dispatch.get(args[0], lambda: op.error("unknown command"))(*args[1:])
        if r is not None:
            _outfile.write(r)
    except TypeError, e:
        op.error("invalid parameters: %s"%e)
        return
    print

if __name__ == "__main__":
    main()
