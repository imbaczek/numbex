import logging
import datetime
import socket
from string import Template

import gitshelve

import crypto
import utils

class NumbexDBError(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)

class NumbexRepo(object):
    def __init__(self, repodir, pubkey_getter, repobranch='numbex'):
        self.repobranch = repobranch
        self.repodir = repodir
        if self.repodir and self.repobranch:
            self.shelf = gitshelve.open(self.repobranch, repository=self.repodir)
        self.get_pubkeys = pubkey_getter
    
    def make_repo_path(self, number):
        '''transform a string like this:

>>> x.make_repo_path('1234567')
'123/456/7' '''
        if len(number)%3 != 0:
            number += ' '*(3 - (len(number)%3))
        return '/'.join(''.join(x) for x in zip(*[iter(number)]*3)).strip()

    def make_record(self, rangestart, rangeend, sip, owner, mdate, sig):
        if isinstance(mdate, datetime.datetime):
            date_modified = mdate.isoformat()
        else:
            date_modified = mdate
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
            ret = [d[x] for x in ('range-start', 'range-end', 'sip-address',
                    'owner', 'date-modified', 'signature')]
            ret[4] = utils.parse_datetime_iso(ret[4])
            return ret
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
        self.sync()
        return True

    def get_range(self, start):
        path = self.make_repo_path(start)
        return self.parse_record(self.shelf[path])

    def export_data_since(self, since):
        pass

    def export_data_all(self):
        ret = []
        for k in self.shelf:
            ret.append(self.parse_record(self.shelf[k]))
        ret.sort(key=lambda x: int(x[0]))
        return ret

    def export_diff(self, rev1, rev2):
        pass

    def add_remote(self, remote, uri):
        return self.shelf.git('remote', 'add', remote, uri)

    def fetch_from_remote(self, remote):
        return self.shelf.git('fetch', remote)

    def merge(self, to_merge):
        # make a tmp repository with a working tree
        # fetch, checkout, merge, push, delete
        # handle conflicts somehow...
        tmpdir = '/tmp/tmprepo1'
        integration = 'integration'
        os.path.mkdir(tmpdir)
        self.shelf.git('branch', integration, to_merge)
        gitshelve.git('init', repository=tmpdir)
        gitshelve.git('remote', 'add', 'origin', self.repodir, repository=tmpdir)
        gitshelve.git('fetch', 'origin', repository=tmpdir)
        gitshelve.git('checkout', '-b', self.repobranch, 'origin/'+self.repobranch, repository=tmpdir)
        gitshelve.git('merge', integration, repository=tmpdir)
        # XXX handle conflicts here
        gitshelve.git('push', 'origin', self.repobranch, repository=tmpdir)
        os.system('rm -rf %s'%tmpdir)


    def sync(self):
        return self.shelf.commit('%s on %s'%(datetime.datetime.now(),
                socket.getfqdn()))

