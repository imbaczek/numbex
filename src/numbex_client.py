#
#
#

from NumbexServiceService_client import *
import sys, time

TRACE=None
loc = NumbexServiceServiceLocator()
port = loc.getNumbexServicePort(url='http://localhost:8000/', tracefile=TRACE)
#port = loc.getNumbexServicePort(url='http://192.168.100.1:8000/echo', tracefile=TRACE)

msg = getData()
msg.Value = 1
rsp = port.getData(msg)
print "INTEGER: ", rsp._return

msg.Value = "HI"
rsp = port.getData(msg)
print "STRING: ", rsp.Value

msg.Value = 1.10000
rsp = port.getData(msg)
print "FLOAT: ", rsp.Value


msg.Value = dict(milk=dict(cost=3.15, unit="gal"))
rsp = port.getData(msg)
print "DICT: ", rsp.Value

msg.Value = list('array')
rsp = port.getData(msg)
print "LIST:", rsp.Value
