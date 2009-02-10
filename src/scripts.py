import os
import datetime

import database
import gitdb

def maketmpdb():
    db = database.Database(':memory:')
    db.create_db()
    db._populate_example()
    return db

def maketmprepo(name, keygetter):
    repo = gitdb.NumbexRepo('/tmp/%s'%name, keygetter)
    return repo


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
        self.repo1.import_data(data)
        self.repo1.sync()
        self.repo2.add_remote('repo1', self.repo1.repodir)
        self.repo2.fetch_from_remote('repo1')
        self.repo2.shelf.git('branch', self.repo1.repobranch, 'repo1/'+self.repo1.repobranch)
        self.repo2.reload()
        self.repo1.import_data([record1])
        self.repo1.sync()
        self.repo2.import_data([record2])
        self.repo2.sync()
        self.repo2.fetch_from_remote('repo1')
        self.repo2.merge('repo1/'+self.repo1.repobranch, dont_push=True)

    def tearDown(self):
        import os
        os.system('rm -rf /tmp/testrepo1')
        os.system('rm -rf /tmp/testrepo2')


