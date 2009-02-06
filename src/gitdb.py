import logging
from string import Template

import gitshelve

import crypto

class NumbexDBError(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)

class NumbexRepo(object):
    def __init__(self, repodir, pubkey_getter):
        self.repodir = repodir
        if repodir:
            self.shelf = gitshelve.open(repodir)
        self.get_pubkeys = pubkey_getter
    
    def make_repo_path(self, number):
        '''transform a string like this:

>>> x.make_repo_path('1234567')
'12/34/56/7' '''
        if len(number)%2 == 1:
            number += ' '
        return '/'.join(''.join(x) for x in zip(*[iter(number)]*2)).strip()

    def make_record(self, rangestart, rangeend, sip, owner, date_modified, sig):
        tmpl = Template('''Range-start: $rangestart
Range-end: $rangeend
Sip-address: $sip
Owner: $owner
Date-modified: $date_modified
Signature: $sig''')
        return tmpl.substitute(locals())

    def parse_record(self, txtrec):
        '''returned data format:
[rangestart, rangeend, sip, owner, date_modified, rsa_signature]'''
        d = {}
        for line in txtrec.splitlines():
            pre, post = line.split(':', 1)
            pre = pre.lower().strip()
            d[pre] = post.strip()
        try:
            return [d[x] for x in ('range-start', 'range-end', 'sip-address',
                    'owner', 'date-modified', 'signature')]
        except KeyError:
            raise NumbexDBError('key not found: '+x)

    def import_data(self, data):
        '''data format: iterator of records in format produced by parse_record'''
        keycache = {}
        for row in data:
            if len(row) != 6:
                logging.warning("invalid record %s", row)
                return False
            owner = row[3]
            if not owner in keycache:
                pemkeys = [k.encode('ascii') if isinstance(k, unicode) else k
                            for k in self.get_pubkeys(owner)]
                keycache[owner] = [crypto.parse_pub_key(k) for k in pemkeys]
                if not keycache[owner]:
                    logging.warning("no key found for %s", row)
                    return False
            checked_data = row[:-1]
            sig = row[-1]
            if not any(crypto.check_signature(key, row[-1], *row[:-1])
                    for key in keycache[owner]):
                logging.warning("invalid signature %s", row)
                return False

        shelf = self.shelf
        for r in data:
            start, end, sip, owner, mdate, rsasig = r
            num = self.make_repo_path(start)
            shelf[num] = self.make_record(start, end, sip, owner, mdate, rsasig)
        shelf.sync()
        return True

    def export_data(self, data):
        pass

    def export_diff(self, rev1, rev2):
        pass

    def fetch_from_uri(self, uri):
        pass

    def merge(self, branch):
        pass
