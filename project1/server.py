import argparse
import xmlrpc.client
import xmlrpc.server

serverId = 0
basePort = 9000

class KVSRPCServer:
  def __init__(self):
    self.store = dict()

  def printKVPairs(self):
    return "\n".join(f"{k}: {v}" for k, v in self.store.items())

  def put(self, key, value):
    self.store[key] = value
    print("Stored", self.store[key])

  def get(self, key):
    if key in self.store:
      return self.store[key]
    else:
      return "ERR_KEY"

  def shutdownServer(self):
    quit()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = '''To be added.''')

    parser.add_argument('-i', '--id', nargs=1, type=int, metavar='I',
                        help='Server id (required)', dest='serverId', required=True)

    args = parser.parse_args()

    serverId = args.serverId[0]

    server = xmlrpc.server.SimpleXMLRPCServer(("localhost", basePort + serverId),
      allow_none=True, use_builtin_types=True)
    server.register_instance(KVSRPCServer())

    server.serve_forever()
