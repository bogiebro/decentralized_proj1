import argparse
import xmlrpc.client
import xmlrpc.server
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer

clientId = 0
basePort = 7000


class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
  pass

class ClientRPCServer:
    def put(self, key, value):
        frontend = xmlrpc.client.ServerProxy("http://localhost:8001")
        return frontend.put(key, value)

    def get(self, key):
        print("Client getting key", key)
        frontend = xmlrpc.client.ServerProxy("http://localhost:8001")
        print("Looking up ", frontend._ServerProxy__host)
        return frontend.get(key)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = '''To be added.''')

    parser.add_argument('-i', '--id', nargs=1, type=int, metavar='I',
                        help='Client id (required)', dest='clientId', required=True)
    args = parser.parse_args()
    clientId = args.clientId[0]

    server = SimpleThreadedXMLRPCServer(("localhost", basePort + clientId), use_builtin_types=True, allow_none=True)
    server.register_instance(ClientRPCServer())

    server.serve_forever()
