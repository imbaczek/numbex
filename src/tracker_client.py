#!/usr/bin/python

import xmlrpclib


def main():
    s = xmlrpclib.ServerProxy('http://localhost:8880')
    print s.echo()

if __name__ == '__main__':
    main()

