import datetime
import re

_isoregex = re.compile('[.:T-]')
def parse_datetime_iso(isostr):
    if not isinstance(isostr, basestring):
        return isostr
    bits = _isoregex.split(isostr)
    try:
        return datetime.datetime(*[int(x) for x in bits])
    except ValueError, e:
        raise ValueError("%s -- %s"%(e, isostr))
