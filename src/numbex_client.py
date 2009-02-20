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
    port = loc.getNumbexServicePort(url=url, tracefile=_tracefile)

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
    try:
        data = filename.read()
    except AttributeError:
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
    if isinstance(csvfile, basestring):
        reader = csv.reader(file(csvfile))
    else:
        reader = csv.reader(csvfile)
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


def get_public_keys(owner, url=None):
    if url is None:
        url = _url

    loc = NumbexServiceServiceLocator()
    port = loc.getNumbexServicePort(url=url, tracefile=_tracefile)

    msg = getPublicKeys()
    msg._parameter = owner
    rsp = port.getPublicKeys(msg)
    return rsp._return


def remove_public_key(keyid, url=None):
    if url is None:
        url = _url

    loc = NumbexServiceServiceLocator()
    port = loc.getNumbexServicePort(url=url, tracefile=_tracefile)

    msg = removePublicKey()
    msg._parameter = keyid
    rsp = port.removePublicKey(msg)


def send_public_key(owner, keyfile, url=None):
    if url is None:
        url = _url

    f = file(keyfile)
    pubkey = f.read()
    f.close()
    dsa = crypto.parse_pub_key(pubkey)
    if not dsa.check_key():
        _tracefile.write('invalid key')
        return

    loc = NumbexServiceServiceLocator()
    port = loc.getNumbexServicePort(url=url, tracefile=_tracefile)

    msg = receivePublicKey()
    msg._owner = owner
    msg._pubkey = pubkey
    rsp = port.receivePublicKey(msg)
    if not rsp._return:
        _tracefile.write('key insertion failed')


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
    send <CSVFILE>   \tsend the contents of CSVFILE as an update
    getpubkeys <OWNER>\tget public keys of OWNER
    sendpubkey <OWNER> <KEYFILE>
                     \tattach public key KEYFILE to an OWNER
    rmpubkey <KEYID> \tdelete public key with id = KEYID
                     \t(check ids with getpubkeys)
    """
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
                'sendsign': send_sign,
                'getpubkeys': get_public_keys,
                'rmpubkey': remove_public_key,
                'sendpubkey': send_public_key,
                }
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
