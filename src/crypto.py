import csv
from M2Crypto import DSA, BIO, EVP
try:
    from cStringIO import StringIO
except ImportError:
    import StringIO


def generate_dsa_key_pair(bits=1024):
    dsa = DSA.gen_params(1024, lambda x:None)
    dsa.gen_key()
    pub = BIO.MemoryBuffer()
    dsa.save_pub_key_bio(pub)
    priv = BIO.MemoryBuffer()
    dsa.save_key_bio(priv, cipher=None)
    return dsa, priv.read(), pub.read()


def sign_record(dsa, start, end, sip, owner, mdate):
    f = StringIO()
    writer = csv.writer(f)
    writer.writerow([start, end, sip, owner, mdate.isoformat()])
    msg = f.getvalue().strip()
    md = EVP.MessageDigest('sha1')
    md.update(msg)
    digest = md.final()
    r, s = dsa.sign(digest)
    return '%s %s %s'%(digest.encode('base64'), r.encode('base64'), s.encode('base64'))


def check_signature(dsapub, sig, start, end, sip, owner, mdate):
    digest, r, s = sig.split()
    digest = digest.decode('base64')
    r = r.decode('base64')
    s = s.decode('base64')
    return dsapub.verify(digest, r, s)

def parse_pub_key(pubstr):
    mem = BIO.MemoryBuffer(pubstr)
    return DSA.load_pub_key_bio(mem)
