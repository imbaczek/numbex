#!/usr/bin/python
import socket, traceback
import logging
import csv
import time
import signal
import sys
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

import database

def make_socket(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, port))
    return s

def serve_numbers_forever(db, host='', port=8990):
    s = make_socket(host, port)
    def sighandler(signum, frame):
        logging.info("received SIGTERM, shutting down.")
        raise SystemExit
    signal.signal(signal.SIGTERM, sighandler)
    logging.info("starting UDP numbex server on port %s", port)
    processed = 0

    while 1:
        try:
            message, address = s.recvfrom(1400)
            if not message.strip():
                continue
            message = message.splitlines()[0].strip()
            logging.info("%s - QUERY %s", address, message)
            start = time.clock()
            r = db.get_range_for(message.strip())
            if r is not None:
                r = list(r)
                r[4] = r[4].isoformat()
                sio = StringIO()
                c = csv.writer(sio)
                c.writerow(r)
                end = time.clock()
                s.sendto("200 OK\n"+sio.getvalue()+"\n", address)
                logging.info("%s - QUERY completed in %.6f s", address, end-start)
            else:
                end = time.clock()
                s.sendto("404 Not found\n", address)
                logging.info("%s - QUERY not found in %.6f s", address, end-start)
            processed += 1

        except (KeyboardInterrupt, SystemExit):
            s.close()
            logging.info("%s queries processed", processed)
            raise
        except socket.error, e:
            s.close()
            s = make_socket(host, port)
            logging.exception('error on %s: %s', address, e)
        except ValueError, e:
            end = time.clock()
            processed += 1
            s.sendto("500 %s\n"%e, address)
            logging.info("%s - QUERY malformed %.6f s", address, end-start)
        except:
            logging.exception('error on %s:')
            try:
                s.sendto("500 Internal error\n", address)
            except socket.error:
                s.close()
                s = make_socket(host, port)
                logging.exception('error on %s: %s', address, e)

def main():
    from optparse import OptionParser
    op = OptionParser(usage="%prog [options]")
    op.add_option("-l", "--loglevel", help="loglevel (DEBUG, WARN)",
        metavar="LOGLEVEL", default="INFO")
    op.add_option("-p", "--port", help="UDP port",
        metavar="PORT", default=8990, type="int", dest="port")
    op.add_option("-b", "--bind-to", help="hostname to bind to",
        metavar="HOST", default="", dest="host")
    options, args = op.parse_args()
    logger = logging.getLogger("")
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    if options.loglevel:
        loglevel = eval(options.loglevel, logging.__dict__)
        logger.setLevel(loglevel)
    if len(args) != 1:
        op.error('incorrect number of arguments; required database filename')
    else:
        db = database.Database(args[0])
    serve_numbers_forever(db, options.host, options.port)


if __name__ == '__main__':
    main()
