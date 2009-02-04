import gitshelve
from string import Template

class NumbexDBError(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)

class NumbexRepo(object):
    def __init__(self, repodir):
        self.repodir = repodir
        if repodir:
            self.shelf = gitshelve.open(repodir)
    
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
        shelf = self.shelf
        for r in data:
            start, end, sip, owner, mdate, rsasig = data
            num = self.make_repo_path(start)
            shelf[num] = self.make_record(start, end, sip, owner, mdate, rsasig)

    def export_data(self, data):
        pass

    def export_diff(self, rev1, rev2):
        pass

    def fetch_from_uri(self, uri):
        pass

    def merge(self, branch):
        pass
