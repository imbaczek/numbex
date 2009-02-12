import datetime
import re
import csv
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

_isoregex = re.compile('[.:T-]')
def parse_datetime_iso(isostr):
    if not isinstance(isostr, basestring):
        return isostr
    bits = _isoregex.split(isostr)
    try:
        return datetime.datetime(*[int(x) for x in bits])
    except ValueError, e:
        raise ValueError("%s -- %s"%(e, isostr))


def parse_csv_data(datastream):
    reader = csv.reader(datastream)
    sio = StringIO()
    ret = []
    for row in reader:
        if len(row) < 5:
            raise ValueError("invalid record: %s"%row)
        row[4] = parse_datetime_iso(row[4])
        ret.append(row)
    return ret
    
