import sqlite3
import os.path
import logging
import time
from datetime import datetime

import crypto
import utils

class Database(object):
    def __init__(self, filename, logger=None, fill_example=None):
        if logger is None:
            self.log = logging.getLogger('database')
            self.log.setLevel(logging.INFO)
        else:
            self.log = logger

        self.conn = None
        self.log.info('starting database with file %s', filename)

        self.filename = filename
        if filename != ':memory:' and not os.path.isfile(filename):
            self.connect(filename)
            self.create_db()
            if fill_example is None:
                fill_example = True
        else:
            self.connect(filename)

        if fill_example:
            self._populate_example()

    def connect(self, filename=None):
        if filename is None:
            filename = self.filename
        self.conn = sqlite3.connect(filename,
                detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def commit(self):
        self.conn.commit()

    def create_db(self):
        self.log.info('creating database tables.')
        c = self.conn.cursor()
        c.execute('''create table numbex_owners (
            name text primary key)''')
        c.execute('''create table numbex_domains (
            sip text primary key,
            owner text,
            foreign key (owner) references numbex_owners(name) )''')
        c.execute('''create table numbex_pubkeys (
            id integer primary key,
            pubkey text,
            owner text,
            foreign key (owner) references numbex_owners(name))''')
        c.execute('''create table numbex_ranges (
            start text primary key,
            end text,
            _s decimal(15,0),
            _e decimal(15,0),
            sip text,
            owner text,
            date_changed timestamp,
            signature text,
            foreign key (sip) references numbex_domains(sip),
            foreign key (owner) references numbex_owners(owner))''')
        c.execute('create unique index ranges_s_index on numbex_ranges(_s)')
        c.execute('create unique index ranges_e_index on numbex_ranges(_e)')

        c.execute('''create table numbex_range_changes (
            id integer primary key,
            start text,
            end text,
            type char(1))''')

        c.close()
        self.conn.commit()

    def drop_db(self):
        self.log.info('dropping database tables.')
        c = self.conn.cursor()
        c.execute('drop table numbex_range_changes')
        c.execute('drop table numbex_ranges')
        c.execute('drop table numbex_pubkeys')
        c.execute('drop table numbex_domains')
        c.execute('drop table numbex_owners')
        c.close()
        self.conn.commit()

    def _populate_example(self):
        self.log.info('generating example data...')
        starttime = time.clock()
        cursor = self.conn.cursor()
        keys = self._populate_example_owners(cursor)
        self._populate_example_ranges(cursor, keys)
        cursor.close()
        endtime = time.clock()
        self.log.info('done, %.3f.', endtime-starttime)


    _example_privkeys = {
            'freeconet': '''-----BEGIN DSA PRIVATE KEY-----
MIIBugIBAAKBgQD3/8OnlvU4Zg9/qKYlhcsRl74g4nSEEkrGEsNdbhiqrnGOusus
B45LZyQkzEG39Z+WfyCLiVGHdJVuYk/YmNEVv9Fd33yLWyc4Xv6rhYUEk29/IksM
pQaxUjamwpu/QKnAt4t6yNT3y0BzwQTiDMPDRNK8MPZUF1pb0itCKko2DwIVAKzg
KetG4mzrlwTLyRf8DRq/4e2nAoGAJ0IrPkybSdRW4+VvfpBUbPOSLjeo0tcSDN47
wTFfvhGedLk/fZlSB+cJqIfyQx+RIxMroHl5hYNteR5pkvnfjD7KgQN1eUW4YJz7
IH1MFEFJmhM41t6xZEndSDwmgWZDD6sp+3MIWGpmsRdOzMVQGZLtqs4x3gt6mwiy
oA7NmpUCgYAFu9Netm2BY1+tYcFaNNhkpVciwYWzEmSAFvFnJajZj1FUsMs2DOtN
GPVkUmKcpL6i1auwRtylEknb2h7Ha6DJKoUeoT7fe0wfhyk44dOcbBV15wYDceOT
XigYaGk3Oi7KR1wdL6sAU0F25F3x+bmYKCV9SvHmUK4XnZ2O4Q0SSAIUXevStm+z
YAQ2Vb07qZIwQ4flH0U=
-----END DSA PRIVATE KEY-----''',
            'telarena': '''-----BEGIN DSA PRIVATE KEY-----
MIIBuwIBAAKBgQC9ot9XYwO4VZLtwFT20KdfEBld5acIKeth5EttDkCZtMFn5lx2
pKtK+mY4sJhPXjb1cL79b5odNQp1ulX77i4qgBsTRJWIRHGLPSMxYcO4af+JchsX
VIcfWE5V/+BdH8BzUNgu7QpE8k+FNcqgMxQpuTJzwyBTcf9yElrBqtj1vQIVAOKk
/AGmUHa6c61qf4TV3J8JPyeZAoGAR/Of2ZA1b3OEpjT6x7aeoiCtjSeedzT4Mh0i
1LEwLYky941N+k9L+S12MPTW32joS3rrYFrNURnQUZXHrGJ/FUgjmWoDphRV943R
dSTR/ylOR7n4auwkEciva8Li+KcjNzgBWPHjVXJStGglZrurx7nPAC7BOGYznSp/
MhO2dV4CgYBgq1w2E5JqJWwWzX7bu9E8aSxhbkC2uaROO9unYjZX13xd9arx70TT
6QRoivNcmtzxAgxL97knGcxGCpBh9S2OpRK787IoaK4WDKCZSTpYOVGs+okCgJiD
J/br8IbfGYoQ0l72Z6ZHwDZMB0E7eNT6QtalgPMncWtbVwnIAGBlFQIVANGyh0sf
W7d77Yq4f2BRkGFp/2Jz
-----END DSA PRIVATE KEY-----''',
        }

    def _populate_example_owners(self, c):
        owners = ['freeconet', 'telarena']
        keys = {}
        ownq = 'insert into numbex_owners (name) values (?)'
        keyq = 'insert into numbex_pubkeys (owner, pubkey) values (?, ?)'
        for o in owners:
            c.execute(ownq, [o])
            priv = self._example_privkeys[o]
            dsa = crypto.parse_priv_key(priv)
            pub = crypto.extract_pub_key(dsa)
            f = file(o+'.priv.pem', 'w')
            f.write(priv)
            f.close()
            f = file(o+'.pem', 'w')
            f.write(pub)
            f.close()
            keys[o] = dsa
            c.execute(keyq, [o, pub])

        domains = [['sip.freeconet.pl:1234', 'freeconet'],
                    ['sip.freeconet.pl', 'freeconet'],
                    ['telarena.pl', 'telarena'],
                    ]
        domainq = 'insert into numbex_domains (sip, owner) values (?, ?)'
        for d in domains:
            c.execute(domainq, d)
        self.conn.commit()
        return keys


    def _populate_example_ranges(self, c, keys = None):
        if keys is None:
            keys = {}
        from datetime import datetime, timedelta
        td1 = timedelta(-10)
        td2 = timedelta(-5)
        now = datetime(2009, 1, 13, 23, 59, 30, 123456)
        data = [('+481234', '+481299', 'sip.freeconet.pl', 'freeconet', now + td1),
                ('+4820000', '+4820999', 'sip.freeconet.pl', 'freeconet', now + td1),
                ('+4830000', '+4830099', 'sip.freeconet.pl', 'freeconet', now + td2),
                ('+4821000', '+4821111', 'sip.freeconet.pl', 'freeconet', now + td2),
                ]
        for r in data:
            if r[3] in keys:
                sig = crypto.sign_record(keys[r[3]], *r)
            else:
                sig = ''
            assert isinstance(r[4], datetime)
            c.execute('''insert into numbex_ranges (start, end, sip, owner, date_changed, signature, _s, _e)
                values (?, ?, ?, ?, ?, ?, ?, ?)''', (list(r)+[sig, int(r[0]), int(r[1])]))
        self.conn.commit()

    def get_data_all(self):
        c = self.conn.cursor()
        c.execute('''select start, end, sip, owner, date_changed, signature
                from numbex_ranges
                order by start''')
        result = list(c)
        c.close()
        return result

    def get_data_since(self, since):
        c = self.conn.cursor()
        c.execute('''select start, end, sip, owner, date_changed, signature
                from numbex_ranges
                where date_changed >= ?
                order by start''', [since])
        result = list(c)
        c.close()
        return result

    def get_data_unsigned(self):
        c = self.conn.cursor()
        c.execute('''select start, end, sip, owner, date_changed, signature
                from numbex_ranges
                where signature = '' or signature is null
                order by start''')
        result = list(c)
        c.close()
        return result

    def get_public_keys(self, owner):
        c = self.conn.cursor()
        keys = self._get_pub_keys(c, owner)
        c.close()
        return list(keys)

    def _get_pub_keys(self, cursor, owner):
        keyq = 'select pubkey from numbex_pubkeys where owner = ?'
        return (x for x, in cursor.execute(keyq, [owner]))

    def _numeric2string(self, num):
        return '+%s'%num

    def check_record_signature(self, cur, start, end, sip, owner, mdate, sig):
        keys = self._get_pub_keys(cur, owner)
        for key in keys:
            dsapub = crypto.parse_pub_key(key.encode('ascii'))
            if crypto.check_signature(dsapub, sig,
                    start, end, sip, owner, mdate):
                return True
        return False

    def check_data_signatures(self, data):
        cursor = self.conn.cursor()
        try:
            for row in data:
                if len(row) != 6:
                    return False
                if not self.check_record_signature(cursor, *row):
                    return False
            return True
        finally:
            cursor.close()

    def _add_change(self, cursor, start, end, tp):
        assert tp in ('D', 'M', 'A')
        q = '''insert into numbex_range_changes (start, end, type)
                values (?, ?, ?)'''
        cursor.execute(q, [start, end, tp])

    def get_changed_data(self):
        q = '''select type, start, end from numbex_range_changes
                order by start'''
        c = self.conn.cursor()
        c.execute(q)
        r = list(c)
        c.close()
        return r

    def get_deleted_data(self):
        q = '''select start, end from numbex_range_changes
               where type = 'D' order by start '''
        c = self.conn.cursor()
        c.execute(q)
        r = list(c)
        c.close()
        return r

    def has_changed_data(self):
        q = '''select count(*)>0 from numbex_range_changes'''
        c = self.conn.cursor()
        c.execute(q)
        r = list(c)[0][0]
        c.close()
        return r

    def clear_changed_data(self):
        q = '''delete from numbex_range_changes'''
        c = self.conn.cursor()
        c.execute(q)
        c.close()
        self.conn.commit()

    def update_data(self, data):
        self.log.info("update data - %s rows", len(data))
        starttime = time.clock()
        # check for overlaps
        data.sort(key=lambda x: int(x[0]))
        prevs, preve = 0, 0
        cursor = self.conn.cursor()
        for row in data:
            s, e = int(row[0]), int(row[1])
            if prevs <= s and preve >= s:
                self.log.error("update data - invalid data %s %s, %s %s",
                        prevs, preve, s, e)
                return False
            # check owners
            overlaps = self.overlapping_ranges(s, e)
            for ovl in overlaps:
                old = self._get_range(cursor, ovl[0])
                if old[3] != row[3]:
                    self.log.error("update data - %s %s overlaps with %s %s" \
                            " which has a different owner (%s vs %s)",
                            s, e, ovl[0], ovl[1], row[3], old[3])
                    return False
            prevs, preve = s, e
        num2str = self._numeric2string
        now = datetime.now()
        for row in data:
            ns, ne = int(row[0]), int(row[1])
            self.log.info('processing [%s]', ', '.join(map(str, row)))
            do_insert = True
            overlaps = self.overlapping_ranges(int(row[0]), int(row[1]))
            for ovl in overlaps:
                os, oe = int(ovl[0]), int(ovl[1])
                # special case: new == old; update DSA signature
                # set signature to '' otherwise
                if os == ns and oe == ne:
                    old = list(self._get_range(cursor, ovl[0]))
                    if old == row:
                        self.log.info('nothing to do for %s %s', ovl[0], ovl[1])
                    elif old[:-1] == row[:-1]:
                        self.log.info('full equal, update sig %s %s',
                                ovl[0], ovl[1])
                        self.set_range_small(cursor, ovl[0], ovl[0], ovl[1],
                                row[4], row[5])
                        self._add_change(cursor, ovl[0], ovl[1], 'M')
                    else:
                        self.log.info('equal ovl %s %s', ovl[0], ovl[1])
                        self.set_range(cursor, ovl[0], *row)
                        self._add_change(cursor, ovl[0], ovl[1], 'M')
                    do_insert = False
                elif os >= ns and os <= ne and oe > ne: # left overlap
                    self.log.info('left ovl %s %s',ovl[0],ovl[1])
                    self.set_range_small(cursor, ovl[0], num2str(ne+1), ovl[1], now)
                    self._add_change(cursor, ovl[0], ovl[1], 'M')
                elif oe >= ns and oe <= ne and os < ns: # right overlap
                    self.log.info('right ovl %s %s',ovl[0],ovl[1])
                    self.set_range_small(cursor, ovl[0], ovl[0], num2str(ns-1), now)
                    self._add_change(cursor, ovl[0], ovl[1], 'M')
                elif os >= ns and oe <= ne: # complete overlap, old is smaller
                    self.log.info('old smaller ovl %s %s',ovl[0],ovl[1])
                    self.delete_range(cursor, ovl[0])
                    self._add_change(cursor, ovl[0], ovl[1], 'D')
                elif os <= ns and oe >= ne: # complete overlap, new is smaller
                    self.log.info('new smaller ovl %s %s',ovl[0],ovl[1])
                    old = self._get_range(cursor, ovl[0])
                    self.set_range_small(cursor, ovl[0], ovl[0], num2str(ns-1), now)
                    self.insert_range(cursor, num2str(ne+1), ovl[1],
                            old[2], old[3], now, '', safe=True)
                    self._add_change(cursor, ovl[0], ovl[1], 'M')
                    self._add_change(cursor, num2str(ne+1), ovl[1], 'A')
            if do_insert:
                row = list(row)
                self.insert_range(cursor, safe=True, *row)
                self._add_change(cursor, row[0], row[1], 'A')
        cursor.close()
        self.conn.commit()
        endtime = time.clock()
        self.log.info("update data complete, %.3f s", endtime-starttime)
        return True

    def overlapping_ranges(self, start, end):
        assert int(start) <= int(end)
        c = self.conn.cursor()
        c.execute('''select start, end
                from numbex_ranges
                where (_s >= ? and _s <= ?) -- left overlap
                    or (_e >= ? and _e <= ?) -- right overlap
                    or (_s <= ? and _e >= ?) -- complete overlap
                    or (_s >= ? and _e <= ?) -- complete overlap''',
                [start, end, start, end, start, end, start, end])
        result = list(c)
        c.close()
        return result

    def get_range(self, start):
        c = self.conn.cursor()
        r = self._get_range(c, start)
        c.close()
        return r

    def _get_range(self, cursor, start):
        r = list(cursor.execute('''select start, end, sip,
                     owner, date_changed, signature
                from numbex_ranges where start = ?''',
                [start]))
        if not r:
            return None
        elif len(r) != 1:
            raise RuntimeError("multiple records found for key %s"%start)
        return r[0]

    def get_range_for(self, number):
        c = self.conn.cursor()
        number = int(number)
        c.execute('''select start, end, sip, owner, date_changed, signature
                from numbex_ranges
                where (_s <= ? and _e >= ?)''',
                [number, number])
        result = list(c)
        c.close()
        if len(result) > 1:
            raise RuntimeError("multiple records found for number %s"%number)
        if result:
            return result[0]
        else:
            return None


    def set_range_small(self, cursor, start, newstart, newend, date_changed, sig=''):
        ns = int(newstart)
        ne = int(newend)
        date_changed = utils.parse_datetime_iso(date_changed)
        self.log.info('set small %s => %s %s',start,newstart,newend)
        assert ns <= ne
        assert isinstance(date_changed, datetime)
        cursor.execute('''update numbex_ranges
                set start = ?, end = ?, _s = ?, _e = ?, date_changed = ?,
                signature = ?
                where start = ?''',
                [newstart, newend, ns, ne, date_changed, sig, start])
        return True

    def set_range(self, cursor, start, newstart, newend, sip, owner, date_changed, sig):
        ns = int(newstart)
        ne = int(newend)
        self.log.info('set full %s => %s %s',start,newstart,newend)
        date_changed = utils.parse_datetime_iso(date_changed)
        assert ns <= ne
        assert isinstance(date_changed, datetime)
        cursor.execute('''update numbex_ranges
                set start = ?, end = ?, _s = ?, _e = ?, sip = ?,
                owner = ?, date_changed = ?, signature = ?
                where start = ?''',
                [newstart, newend, ns, ne, sip, owner, date_changed, sig, start])
        return True

    def insert_range(self, cursor, start, end, sip, owner, date_changed, sig,
                safe=True):
        ns = int(start)
        ne = int(end)
        date_changed = utils.parse_datetime_iso(date_changed)
        ovl = self.overlapping_ranges(start, end)
        if safe and ovl:
            raise ValueError("cannot insert range %s-%s: overlaps with %s"%
                (start, end, str(ovl)))
        self.log.info('insert %s %s',start,end)
        assert ns <= ne
        assert isinstance(date_changed, datetime)
        cursor.execute('''insert into numbex_ranges
                (start, end, _s, _e, sip, owner, date_changed, signature)
                values (?, ?, ?, ?, ?, ?, ?, ?)''',
                [start, end, ns, ne, sip, owner, date_changed, sig])
        return True

    def delete_range(self, cursor, start):
        self.log.info('delete %s',start)
        cursor.execute('''delete from numbex_ranges
                where start = ?''',
                [start])
        return True

    def ranges_empty(self):
        q = 'select count(*)=0 from numbex_ranges'
        c = self.conn.cursor()
        c.execute(q)
        r = list(c)[0][0]
        c.close()
        return r

    
    def get_public_keys_ids(self, owner):
        q = 'select id, pubkey from numbex_pubkeys where owner = ?'
        c = self.conn.cursor()
        c.execute(q, [owner])
        r = list(c)
        c.close()
        return r

    def remove_public_key(self, keyid):
        q = 'delete from numbex_pubkeys where id = ?'
        c = self.conn.cursor()
        c.execute(q, [keyid])
        r = list(c)
        c.close()
        self.conn.commit()
        return r

    def add_public_key(self, owner, pubkey):
        q = '''insert into numbex_pubkeys (owner, pubkey)
               values (?, ?)'''
        c = self.conn.cursor()
        c.execute(q, [owner, pubkey])
        c.close()
        self.conn.commit()


