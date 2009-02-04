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


def make_csv_record(start, end, sip, owner, mdate):
    f = StringIO()
    writer = csv.writer(f)
    writer.writerow([start, end, sip, owner, mdate.isoformat()])
    return f.getvalue().strip()

def sign_record(dsa, start, end, sip, owner, mdate):
    return sign_csv_record(dsa, make_csv_record(start, end, sip, owner, mdate))

def sign_csv_record(dsa, msg):
    md = EVP.MessageDigest('sha1')
    md.update(msg)
    digest = md.final()
    r, s = dsa.sign(digest)
    return '%s %s'%(r.encode('base64').strip(), s.encode('base64').strip())

def check_signature(dsapub, sig, start, end, sip, owner, mdate):
    return check_csv_signature(dsapub, sig,
                make_csv_record(start, end, sip, owner, mdate))

def check_csv_signature(dsapub, sig, msg):
    md = EVP.MessageDigest('sha1')
    md.update(msg)
    digest = md.final()
    r, s = sig.split()
    r = r.decode('base64')
    s = s.decode('base64')
    return dsapub.verify(digest, r, s)

def parse_pub_key(pubstr):
    mem = BIO.MemoryBuffer(pubstr)
    return DSA.load_pub_key_bio(mem)
