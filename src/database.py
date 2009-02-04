import sqlite3
import os.path
import csv
from datetime import datetime

import crypto

class Database(object):
    def __init__(self, filename):
        self.filename = filename
        if not os.path.isfile(filename):
            self.conn = sqlite3.connect(filename,
                    detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
            self.create_db()
            self._populate_example()
        else:
            self.conn = sqlite3.connect(filename,
                    detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)

    def create_db(self):
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

        self.conn.commit()
        c.close()

    def _populate_example(self):
        print 'generating example data...'
        cursor = self.conn.cursor()
        keys = self._populate_example_owners(cursor)
        self._populate_example_ranges(cursor, keys)
        print 'done.'
        cursor.close()

    def _populate_example_owners(self, c):
        from M2Crypto import DSA, BIO

        owners = ['freeconet', 'telarena']
        keys = {}
        ownq = 'insert into numbex_owners (name) values (?)'
        keyq = 'insert into numbex_pubkeys (owner, pubkey) values (?, ?)'
        for o in owners:
            c.execute(ownq, [o])
            dsa, priv, pub = crypto.generate_dsa_key_pair(1024)
            f = file(o+'.priv.pem', 'w')
            f.write(priv)
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
        now = datetime.now()
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
            c.execute('''insert into numbex_ranges (start, end, sip, owner, date_changed, signature, _s, _e)
                values (?, ?, ?, ?, ?, ?, ?, ?)''', (list(r)+[sig, int(r[0]), int(r[1])]))
        self.conn.commit()

    def get_data_all(self):
        c = self.conn.cursor()
        c.execute('''select start, end, sip, date_changed
                from numbex_ranges
                order by start''')
        result = list(c)
        c.close()
        return result

    def get_data_since(self, since):
        c = self.conn.cursor()
        c.execute('''select start, end, sip, date_changed
                from numbex_ranges
                where date_changed >= ?
                order by start''', [since])
        result = list(c)
        c.close()
        return result

    def _numeric2string(self, num):
        return '+%s'%num

    def update_data(self, data):
        cursor = self.conn.cursor()
        num2str = self._numeric2string
        now = datetime.now()
        for row in data:
            ns, ne = int(row[0]), int(row[1])
            print '***',row
            for ovl in self.overlapping_ranges(int(row[0]), int(row[1])):
                os, oe = int(ovl[0]), int(ovl[1])
                if os >= ns and os <= ne and oe >= ne: # left overlap
                    print 'left ovl',ovl[0],ovl[1]
                    self.set_range(cursor, ovl[0], num2str(ne+1), ovl[1], now)
                elif oe >= ns and oe <= ne and os <= ns: # right overlap
                    print 'right ovl',ovl[0],ovl[1]
                    self.set_range(cursor, ovl[0], ovl[0], num2str(ns-1), now)
                elif os >= ns and oe <= ne: # complete overlap, old is smaller
                    print 'old smaller ovl',ovl[0],ovl[1]
                    self.delete_range(cursor, ovl[0])
                elif os <= ns and oe >= ne: # complete overlap, new is smaller
                    print 'new smaller ovl',ovl[0],ovl[1]
                    old = self.get_range(cursor, ovl[0])
                    self.set_range(cursor, ovl[0], ovl[0], num2str(ns-1), now)
                    self.insert_range(cursor, num2str(ne+1), ovl[1],
                            old[2], now)
            row[3] = now
            self.insert_range(cursor, *row)
        self.conn.commit()
        cursor.close()
        return True

    def overlapping_ranges(self, start, end):
        assert start <= end
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

    def get_range(self, cursor, start):
        return list(cursor.execute('''select start, end, sip, date_changed
                from numbex_ranges where start = ?''',
                [start]))[0]

    def set_range(self, cursor, start, newstart, newend, date_changed):
        ns = int(newstart)
        ne = int(newend)
        print 'set',start,'=>',newstart,newend
        assert ns <= ne
        cursor.execute('''update numbex_ranges
                set start = ?, end = ?, _s = ?, _e = ?, date_changed = ?
                where start = ?''',
                [newstart, newend, ns, ne, date_changed, start])
        return True

    def insert_range(self, cursor, start, end, sip, date_changed):
        ns = int(start)
        ne = int(end)
        if isinstance(date_changed, str):
            if '.' in date_changed:
                date_changed = date_changed[:-7]
            date_changed = datetime.strptime(date_changed, "%Y-%m-%dT%H:%M:%S")
        print 'insert',start,end
        assert ns <= ne
        assert isinstance(date_changed, datetime)
        cursor.execute('''insert into numbex_ranges
                (start, end, _s, _e, sip, date_changed)
                values (?, ?, ?, ?, ?, ?)''',
                [start, end, ns, ne, sip, date_changed])
        return True

    def delete_range(self, cursor, start):
        print 'delete',start
        cursor.execute('''delete from numbex_ranges
                where start = ?''',
                [start])
        return True
