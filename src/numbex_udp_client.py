#!/usr/bin/python
import socket, traceback
import logging
import csv
import sys

class UDPServerException(Exception):
    def __init__(*args, **kwargs):
        Exception.__init__(*args, **kwargs)

def query_server(number, host, port, timeout=1.0):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(timeout)
    s.sendto(number, (host, port))
    message, address = s.recvfrom(1400)
    s.close()
    if message.startswith('200'):
        return message.splitlines()[1].strip()
    elif message.startswith('404'):
        return None
    else:
        raise UDPServerException(message)


def main():
    from optparse import OptionParser
    op = OptionParser(usage="%prog [options]")
    op.add_option("-p", "--port", help="UDP destination port",
        metavar="PORT", default=8990, type="int", dest="port")
    op.add_option("-t", "--timeout", help="timeout",
        metavar="TIMEOUT", default=8990, type="int")
    options, args = op.parse_args()
    if len(args) != 2:
        op.error('invalid number of arguments; expected host and e.164 number')
    host = args[0]
    number = args[1]
    try:
        r = query_server(number, host, options.port, timeout=options.timeout)
        if r:
            print r
        else:
            print 'requested number not found in database'
            sys.exit(1)
    except UDPServerException, e:
        sys.stderr.write('server error: %s\n'%e)
        sys.exit(1)
    except socket.timeout:
        sys.stderr.write('server error: server timed out\n')
        sys.exit(1)
    except IOError, e:
        sys.stderr.write('io error: %s\n'%e)
        sys.exit(1)



if __name__ == '__main__':
    main()
