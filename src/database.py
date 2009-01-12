import sqlite3
import os.path

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
        c.execute('''create table zakresy (
            start text primary key,
            end text,
            sip text,
            date_changed timestamp)''')
        self.conn.commit()
        c.close()

    def _populate_example(self):
        from datetime import datetime, timedelta
        td1 = timedelta(-10)
        td2 = timedelta(-5)
        now = datetime.now()
        data = [('+481234', '+481299', 'sip.freeconet.pl', now + td1),
                ('+4820000', '+4820999', 'sip.freeconet.pl', now + td1),
                ('+4830000', '+4830099', 'sip.freeconet.pl', now + td2),
                ('+4821000', '+4821111', 'sip.freeconet.pl', now + td2),
                ]
        c = self.conn.cursor()
        for r in data:
            c.execute('''insert into zakresy (start, end, sip, date_changed)
                values (?, ?, ?, ?)''', r)
        self.conn.commit()
        c.close()

    def get_data_all(self):
        c = self.conn.cursor()
        c.execute('''select start, end, sip, date_changed
                from zakresy
                order by start''')
        result = [row for row in c]
        c.close()
        return result

    def get_data_since(self, since):
        c = self.conn.cursor()
        c.execute('''select start, end, sip, date_changed
                from zakresy
                where date_changed >= ?
                order by start''', since)
        result = [row for row in c]
        c.close()
        return result
        
