import argparse
import xmlrpc.client
import xmlrpc.server
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer
import logging

clientId = 0
basePort = 7000

frontend = xmlrpc.client.ServerProxy("http://localhost:8001")

class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
  pass

class ClientRPCServer:
    def put(self, key, value):
        return frontend.put(key, value)

    def get(self, key):
        print("Client getting key", key)
        print("Looking up ", frontend._ServerProxy__host)
        return frontend.get(key)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = '''To be added.''')

    parser.add_argument('-i', '--id', nargs=1, type=int, metavar='I',
                        help='Client id (required)', dest='clientId', required=True)
    args = parser.parse_args()
    clientId = args.clientId[0]
    logging.basicConfig(filename=f"client{clientId}.log", level=logging.DEBUG)

    logging.info(f"Listening on {basePort + clientId}") 

    server = SimpleThreadedXMLRPCServer(("localhost", basePort + clientId), use_builtin_types=True, allow_none=True)
    server.register_instance(ClientRPCServer())

    server.serve_forever()
