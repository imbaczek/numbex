#!/usr/bin/python

import logging
import time
import SocketServer
import fcntl
import traceback
import xmlrpclib
import sys
import threading
from SimpleXMLRPCServer import SimpleXMLRPCServer, SimpleXMLRPCDispatcher, \
        SimpleXMLRPCRequestHandler, Fault


class NumbexXMLRPCDispatcher(SimpleXMLRPCDispatcher):
    def _marshaled_dispatch(self, client_address, data, dispatch_method = None):
        """Dispatches an XML-RPC method from marshalled (XML) data.

        XML-RPC methods are dispatched from the marshalled (XML) data
        using the _dispatch method and the result is returned as
        marshalled data. For backwards compatibility, a dispatch
        function can be provided as an argument (see comment in
        SimpleXMLRPCRequestHandler.do_POST) but overriding the
        existing method through subclassing is the prefered means
        of changing method dispatch behavior.
        """

        try:
            params, method = xmlrpclib.loads(data)
            params = (client_address,)+params
            # generate response
            if dispatch_method is not None:
                response = dispatch_method(method, params)
            else:
                response = self._dispatch(method, params)
            # wrap response in a singleton tuple
            response = (response,)
            response = xmlrpclib.dumps(response, methodresponse=1,
                                       allow_none=self.allow_none, encoding=self.encoding)
        except Fault, fault:
            response = xmlrpclib.dumps(fault, allow_none=self.allow_none,
                                       encoding=self.encoding)
        except:
            traceback.print_exc()
            # report exception back to server
            response = xmlrpclib.dumps(
                xmlrpclib.Fault(1, "%s:%s" % (sys.exc_type, sys.exc_value)),
                encoding=self.encoding, allow_none=self.allow_none,
                )

        return response

class NumbexXMLRPCRequestHandler(SimpleXMLRPCRequestHandler):
    def do_POST(self):
        """Handles the HTTP POST request.

        Attempts to interpret all HTTP POST requests as XML-RPC calls,
        which are forwarded to the server's _dispatch method for handling.
        """

        # Check that the path is legal
        if not self.is_rpc_path_valid():
            self.report_404()
            return

        try:
            # Get arguments by reading body of request.
            # We read this in chunks to avoid straining
            # socket.read(); around the 10 or 15Mb mark, some platforms
            # begin to have problems (bug #792570).
            max_chunk_size = 10*1024*1024
            size_remaining = int(self.headers["content-length"])
            L = []
            while size_remaining:
                chunk_size = min(size_remaining, max_chunk_size)
                L.append(self.rfile.read(chunk_size))
                size_remaining -= len(L[-1])
            data = ''.join(L)

            # In previous versions of SimpleXMLRPCServer, _dispatch
            # could be overridden in this class, instead of in
            # SimpleXMLRPCDispatcher. To maintain backwards compatibility,
            # check to see if a subclass implements _dispatch and dispatch
            # using that method if present.
            # XXX the only change to default handler is here
            response = self.server._marshaled_dispatch(
                    self.client_address, data, getattr(self, '_dispatch', None)
                )
        except: # This should only happen if the module is buggy
            # internal error, report as HTTP server error
            traceback.print_exc()
            self.send_response(500)
            self.end_headers()
        else:
            # got a valid XML RPC response
            self.send_response(200)
            self.send_header("Content-type", "text/xml")
            self.send_header("Content-length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

            # shut down the connection
            self.wfile.flush()
            self.connection.shutdown(1)

class NumbexXMLRPCServer(SocketServer.TCPServer,
                         NumbexXMLRPCDispatcher):
    """Simple XML-RPC server.

    Simple XML-RPC server that allows functions and a single instance
    to be installed to handle requests. The default implementation
    attempts to dispatch XML-RPC calls to the functions or instance
    installed in the server. Override the _dispatch method inhereted
    from SimpleXMLRPCDispatcher to change this behavior.
    """

    allow_reuse_address = True

    def __init__(self, addr, requestHandler=NumbexXMLRPCRequestHandler,
                 logRequests=True, allow_none=False, encoding=None):
        self.logRequests = logRequests

        NumbexXMLRPCDispatcher.__init__(self, allow_none, encoding)
        SocketServer.TCPServer.__init__(self, addr, requestHandler)

        # [Bug #1222790] If possible, set close-on-exec flag; if a
        # method spawns a subprocess, the subprocess shouldn't have
        # the listening socket open.
        if fcntl is not None and hasattr(fcntl, 'FD_CLOEXEC'):
            flags = fcntl.fcntl(self.fileno(), fcntl.F_GETFD)
            flags |= fcntl.FD_CLOEXEC
            fcntl.fcntl(self.fileno(), fcntl.F_SETFD, flags)


# TODO add locks
class NumbexTracker(object):
    def __init__(self, auth_function=None, timeout=300):
        self.peers = {}
        self.auth_function = auth_function
        self.timeout = timeout
        self.running = True

    def register(self, client_address, user, authdata, advertised_address):
        logging.info("peer register: %s", advertised_address)
        address = self._check_address(client_address, advertised_address)
        self.peers[address] = time.time()
        return self.timeout

    def _check_address(self, client, advertised):
        hc = client[0]
        if isinstance(advertised, basestring):
            ha = advertised
        else:
            try:
                ha = advertised[0]
            except:
                ha = None
        if hc != ha:
            logging.warn('peer advertised host %s != connect host %s', ha, hc)
        try:
            addr, port = advertised
            if not isinstance(addr, basestring) and not isinstance(int, port):
                return None, None
            else:
                return addr, port
        except ValueError:
            return advertised


    def keepalive(self, client_address, advertised_address):
        logging.info("peer keepalive: %s", advertised_address)
        address = self._check_address(client_address, advertised_address)
        if address in self.peers:
            self.peers[address] = time.time()
            return True
        else:
            logging.warn("spurious keepalive from %s as %s",
                    client_address, advertised_address)
            return False

    def get_peers(self, client_address, advertised_address):
        address = self._check_address(client_address, advertised_address)
        return [x for x in self.peers if x != address]
    
    def unregister(self, client_address, advertised_address):
        logging.info("peer unregister: %s", advertised_address)
        address = self._check_address(client_address, advertised_address)
        if address in self.peers:
            del self.peers[address]
        else:
            logging.warn("spurious unregister from %s as %s",
                    client_address, advertised_address)
        return True

    def echo(self, client_address):
        logging.info('echo %s', client_address)
        return client_address

    def _clean_timeouted(self):
        t = time.time()
        todel = []
        for k in self.peers:
            if self.peers[k] + self.timeout < t:
                logging.info("peer timeout: %s", k)
                todel.append(k)
        for k in todel:
            del self.peers[k]


def mainloop(host, port, timeout):
    instance = NumbexTracker(timeout=timeout)
    
    def clean():
        while instance.running:
            instance._clean_timeouted()
            time.sleep(5)

    t = threading.Thread(target=clean)
    t.daemon = True
    srv = NumbexXMLRPCServer((host, port))
    srv.register_instance(instance)
    logging.info("starting Numbex XML RPC tracker server.")
    t.start()
    try:
        srv.serve_forever()
    except (KeyboardInterrupt, SystemExit):
        instance.running = False
        logging.info("clean shutdown")
        sys.exit(0)
    except:
        instance.running = False
        logging.exception("shutting down due to exception")
        raise
    

def main():
    from optparse import OptionParser
    op = OptionParser(usage="%prog [options]")
    op.add_option("-p", "--port", help="UDP port",
        metavar="PORT", default=8880, type="int", dest="port")
    op.add_option("-b", "--bind-to", help="hostname to bind to",
        metavar="HOST", default="", dest="host")
    op.add_option("-t", "--peer-timeout", type="int",
        metavar="TIMEOUT", default=300)
    options, args = op.parse_args()

    logging.basicConfig(level=logging.INFO)
    mainloop(options.host, options.port, options.peer_timeout)



if __name__ == '__main__':
    main()
