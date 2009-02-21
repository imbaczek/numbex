import os
import datetime
import csv

import database
import gitdb
import crypto

def recsign(r, keyfile='freeconet.priv.pem'):
    from M2Crypto import DSA
    import crypto
    dsa = DSA.load_key(keyfile)
    return crypto.sign_record(dsa, *r)

def maketmpdb():
    db = database.Database(':memory:')
    db.create_db()
    db._populate_example()
    return db

def maketmprepo(name, keygetter):
    repo = gitdb.NumbexRepo('/tmp/%s'%name, keygetter)
    return repo


def generatedata(n, owner="freeconet", keyfile="freeconet.priv.pem"):
    start = 48600000000
    end   = 48699999999
    thissip = 'sip.freeconet.pl'
    prevsip = 'new.freeconet.pl'
    thisdate = datetime.datetime(2009, 2, 14, 12).isoformat()
    prevdate = datetime.datetime(2009, 2, 15, 9).isoformat()
    from random import randrange
    # no duplicates
    points = list(set(randrange(start, end) for i in xrange(n)))
    points.sort()
    first = ['+%s'%start,
            '+%s'%points[0],
            thissip,
            owner,
            thisdate]
    from M2Crypto import DSA
    dsa = DSA.load_key(keyfile)
    first.append(crypto.sign_record(dsa, *first))
    data = [first]
    for i in xrange(n-1):
        print i, i+1, len(points), points[i]
        s = points[i]
        e = points[i+1]-1
        thissip, prevsip = prevsip, thissip
        thisdate, prevdate = prevdate, thisdate
        r = ['+%s'%s, '+%s'%e, thissip, owner, thisdate]
        r.append(crypto.sign_record(dsa, *r))
        data.append(r)
    return data

def writecsv(data, ofile):
    out = csv.writer(ofile)
    for r in data:
        out.writerow(r)


class TestRepo(object):
    def setUp(self):
        # db is only needed for keys
        self.db = database.Database(':memory:')
        self.db.create_db()
        self.db._populate_example()
        self.repo1 = gitdb.NumbexRepo('/tmp/testrepo1', self.db.get_public_keys)
        self.repo2 = gitdb.NumbexRepo('/tmp/testrepo2', self.db.get_public_keys)
        self.setUpData()

    def setUpData(self):
        data = [['+481000',
                 '+481500',
                 'sip.freeconet.pl',
                 'freeconet',
                 datetime.datetime(2009, 2, 9, 16, 51, 20, 322133),
                 'AAAAFQCHHDGoHCotDhRjwamVnIXVrehHag== AAAAFA+Ov6nj5V2LugDEHOhfAwDvSb/1'],
                ['+482500',
                 '+483000',
                 'sip.freeconet.pl',
                 'freeconet',
                 datetime.datetime(2009, 2, 9, 19, 31, 48, 590016),
                 'AAAAFFA1HQw+i7ZIRCoECkDDy3ehCYHc AAAAFFoaeviJLijxAHb0qFfLyUjJXX/T']
            ]
        record1 = ['+484000',
                 '+484999',
                 'sip.freeconet.pl',
                 'freeconet',
                 datetime.datetime(2009, 2, 10, 12, 15, 43, 903607),
                 'AAAAFQCn3vHSuLoxcsc9dJaFj+M989sNIQ== AAAAFAivBp7N8tBAQpXxeci3VQ3S0L8F']
        record2 = ['+484000',
                 '+484999',
                 'new.freeconet.pl',
                 'freeconet',
                 datetime.datetime(2009, 2, 10, 12, 22, 43, 67814),
                 'AAAAFF+4ccxb+ihQFxOUTjF3uddu7utk AAAAFEn8PyJE8CkkYLubyLgbhQRbyTBq']
        record3 = ['+485000',
                 '+485500',
                 'new.freeconet.pl',
                 'freeconet',
                 datetime.datetime(2009, 2, 11, 0, 15, 17, 457868),
                 'AAAAFQCWbSY9RGhJrFMroQnJhPtozMz2gA== AAAAFQCrULccnUwPwmQiIiv10oC26OdOKw==']

        self.repo1.import_data(data)
        self.repo1.sync()
        self.repo2.add_remote('repo1', self.repo1.repodir)
        self.repo2.fetch_from_remote('repo1')
        self.repo2.shelf.git('branch', self.repo1.repobranch, 'repo1/'+self.repo1.repobranch)
        self.repo2.reload()
        self.repo1.import_data([record1])
        self.repo1.sync()
        self.repo2.import_data([record2, record3])
        self.repo2.sync()
        self.repo2.fetch_from_remote('repo1')
        self.repo2.merge('repo1/'+self.repo1.repobranch)
        self.repo2.reload()

    def tearDown(self):
        import os
        os.system('rm -rf /tmp/testrepo1')
        os.system('rm -rf /tmp/testrepo2')


