import logging
import datetime
import socket
import os
import tempfile
from string import Template
import subprocess
import signal
import time

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
        else:
            self.shelf = None
        self.get_pubkeys = pubkey_getter
        self.daemon = None
        self.log = logging.getLogger("git")
    
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
        self.log.info('importing %s records...', len(data))
        tstart = time.time()
        keycache = {}
        for row in data:
            if len(row) != 6:
                self.log.warning("invalid record %s", row)
                return False
            owner = row[3]
            if not owner in keycache:
                pemkeys = [k.encode('ascii') if isinstance(k, unicode) else k
                            for k in self.get_pubkeys(owner)]
                keycache[owner] = [crypto.parse_pub_key(k) for k in pemkeys]
                if not keycache[owner]:
                    self.log.warning("no key found for %s", row)
                    return False
            checked_data = row[:-1]
            sig = row[-1]
            if not any(crypto.check_signature(key, row[-1], *row[:-1])
                    for key in keycache[owner]):
                self.log.warning("invalid signature %s", row)
                return False

        shelf = self.shelf
        for r in data:
            start, end, sip, owner, mdate, rsasig = r
            num = self.make_repo_path(start)
            newrec = self.make_record(start, end, sip, owner, mdate, rsasig)
            # 'key in shelf' doesn't seem to work
            try:
                oldrec = shelf[num]
            except KeyError:
                shelf[num] = newrec
            else:
                if oldrec != newrec:
                    shelf[num] = newrec
        self.sync()
        tend = time.time()
        self.log.info("import successful, time %.3f", tend-tstart)
        return True

    def get_range(self, start):
        path = self.make_repo_path(start)
        return self.parse_record(self.shelf[path])

    def export_data_all(self):
        ret = []
        if self.shelf is None:
            return []
        for k in self.shelf:
            ret.append(self.parse_record(self.shelf[k]))
        ret.sort(key=lambda x: int(x[0]))
        return ret

    def export_data_since(self, since):
        if not isinstance(since, datetime.datetime):
            raise TypeError('since is type %s, expected datetime.datetime'%type(since))
        if self.shelf is None:
            return []
        ret = []
        shelf = self.shelf
        for k in self.shelf:
            rec = self.parse_record(shelf[k])
            if rec[4] >= since:
                ret.append(rec)
        ret.sort(key=lambda x: int(x[0]))
        return ret

    def export_diff(self, rev1, rev2):
        pass

    def get_remotes(self):
        r = self.shelf.git('remote', '-v', 'show')
        remotes = {}
        for line in r.splitlines():
            name, url = line.split('\t', 1)
            remotes[name] = url
        return remotes

    def add_remote(self, remote, uri, force=False):
        remotes = self.get_remotes()
        if remote in remotes:
            if remotes[remote] == uri:
                return
            else:
                if not force:
                    raise ValueError('remote %s already exists'%remote)
                else:
                    self.shelf.git('remote', 'rm', remote)
        self.shelf.git('remote', 'add', remote, uri)

    def fetch_from_remote(self, remote):
        return self.shelf.git('fetch', remote)

    @staticmethod
    def clone_uri(uri, destdir, origin_name):
        return gitshelve.git('clone', '--bare', '-o', origin_name,
                uri, destdir)

    def merge(self, to_merge, dont_push=False):
        # make a tmp repository with a working tree
        # fetch, checkout, merge, push, delete
        # handle conflicts somehow...
        tmpdir = tempfile.mkdtemp(prefix='numbex-integration')
        tmprepo = os.path.join(tmpdir, '.git')
        integration = 'integration'
        had_conflicts = False
        self.log.info("starting merge")
        try:
            # create a local branch which we want to merge
            self.shelf.git('branch', integration, to_merge)
            gitshelve.git('init', repository=tmprepo)
            gitshelve.git('remote', 'add', 'origin', self.repodir,
                    repository=tmprepo)
            gitshelve.git('fetch', 'origin', repository=tmprepo)

            # prepare to merge
            gitshelve.git('checkout', '-b', self.repobranch,
                    'origin/'+self.repobranch, repository=tmprepo,
                    worktree=tmpdir)
            cwd = os.getcwd()
            # GIT_WORK_TREE doesn't work with git merge (bug in git?)
            os.chdir(tmpdir)
            try:
                mergeout = gitshelve.git('merge', 'origin/'+integration,
                        repository=tmprepo, worktree=tmpdir)
            except gitshelve.GitError, e:
                # check if merge failed due to a conflict or something else
                if 'Automatic merge failed; fix conflicts and then commit the result.' in str(e):
                    # handle conflicts here
                    # git status fails with error code 128 for some reason
                    # but generates required output
                    status = gitshelve.git('status', repository=tmprepo,
                            worktree=tmpdir, ignore_errors=True)
                    for line in status.splitlines():
                        if line.startswith('# '):
                            break
                        filename, status = line.rsplit(':', 1)
                        filename = filename.strip()
                        status = status.strip()
                        if status == 'needs merge':
                            had_conflicts = True
                            self.log.info("conflicted file: %s", filename)
                            self.handle_merge(os.path.abspath(filename))
                            gitshelve.git('add', filename)
                    # commit the result
                    gitshelve.git('commit', '-m', 'merge. %s on %s'%
                            (datetime.datetime.now(), socket.getfqdn()),
                            repository=tmprepo, worktree=tmpdir)
                else:
                    # not a merge conflict, abort
                    raise
            except:
                self.log.exception("'git merge' failed:")
                raise
            finally:
                os.chdir(cwd)
            # push the results
            if not dont_push:
                gitshelve.git('push', 'origin', self.repobranch,
                        repository=tmprepo)
            if had_conflicts:
                self.log.info("merge completed with conflicts resolved")
            else:
                self.log.info("merge completed without conflicts")
            return had_conflicts
        except:
            self.log.exception("merge failed:")
            raise
        finally:
            os.system('rm -rf %s'%tmpdir)
            # delete the local branch
            try:
                self.shelf.git('branch', '-D', integration)
            except gitshelve.GitError:
                pass

    def handle_merge(self, filename):
        f = file(filename)
        contents = f.readlines()
        f.close()
        ver1 = []
        ver2 = []
        current = 'both'
        startv1 = '<<<<<<<'
        startboth = '>>>>>>>'
        startv2 = '======='
        for line in contents:
            line = line.strip()
            if line.startswith(startv1):
                current = 'v1'
                continue
            elif line.startswith(startv2):
                current = 'v2'
                continue
            elif line.startswith(startboth):
                current = 'both'
                continue
            if current == 'both':
                ver1.append(line)
                ver2.append(line)
            elif current == 'v1':
                ver1.append(line)
            else:
                ver2.append(line)
        v1 = '\n'.join(ver1)
        v2 = '\n'.join(ver2)
        rec1 = self.parse_record(v1)
        rec2 = self.parse_record(v2)
        # check signatures
        if not any(crypto.check_signature(crypto.parse_pub_key(k.encode()),
                rec1[5], *rec1)
                for k in self.get_pubkeys(rec1[3])):
            raise NumbexDBError('invalid signature on %s'%rec1)
        if not any(crypto.check_signature(crypto.parse_pub_key(k.encode()),
                rec2[5], *rec2)
                for k in self.get_pubkeys(rec2[3])):
            raise NumbexDBError('invalid signature on %s'%rec2)
        # compare owners
        if rec1[3] != rec2[3]:
            raise NumbexDBError('cannot merge %s and %s - owners differ'%(rec1, rec2))
        # choose the more recent version
        from operator import itemgetter
        merged = max(rec1, rec2, key=itemgetter(4))
        mergedtxt = self.make_record(*merged)
        f = file(filename, 'wb')
        f.write(mergedtxt)
        f.close()


    def sync(self):
        return self.shelf.commit('%s on %s'%(datetime.datetime.now(),
                socket.getfqdn()))

    def reload(self):
        if self.shelf is not None:
            self.shelf.read_repository()

    def start_daemon(self, port):
        f = file(os.path.join(self.repodir, 'git-daemon-export-ok'), 'a')
        f.close()
        self.daemon = subprocess.Popen(('git', 'daemon', '--reuseaddr',
                '--base-path='+self.repodir, '--port=%s'%port, self.repodir))
        time.sleep(0.2)
        if self.daemon.poll() is None:
            self.log.info("started git-daemon (pid %s) on port %s",
                    self.daemon.pid, port)
            return True
        else:
            self.log.error("couldn't start git-daemon, exit code %s: %s",
                    self.daemon.returncode, self.daemon.stderr.read())
            return False

    def stop_daemon(self):
        if self.daemon is not None:
            self.log.info("terminating daemon")
            os.kill(self.daemon.pid, signal.SIGTERM)
            time.sleep(1)
            if self.daemon.poll() is None:
                self.log.info("killing daemon")
                os.kill(self.daemon.pid, signal.SIGKILL)
                self.daemon = None

    def dispose(self):
        self.sync()
        self.shelf.close()
        self.pubkey_getter = None
        self.stop_daemon()

    def __nonzero__(self):
        i = iter(self.shelf)
        try:
            i.next()
        except StopIteration:
            return False
        else:
            return True
