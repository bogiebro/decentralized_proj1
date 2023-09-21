import argparse
import xmlrpc.client
import xmlrpc.server
import logging
import time

serverId = 0
basePort = 9000

class KVSRPCServer:
  def __init__(self):
    self.store = dict()

  def getAll(self):
    return self.store 

  def put(self, key, value):
    self.store[key] = value
    logging.info(f"Stored {self.store[key]}")

  def putAll(self, d):
    self.store.update(d)

  def get(self, key):
    time.sleep(0.02) # so parallel processing shows
    if key in self.store:
      return self.store[key]
    else:
      return "ERR_KEY"

  def shutdownServer(self):
    quit()

  def beat(self):
    return ""

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = '''To be added.''')

    parser.add_argument('-i', '--id', nargs=1, type=int, metavar='I',
                        help='Server id (required)', dest='serverId', required=True)

    args = parser.parse_args()

    serverId = args.serverId[0]

    logging.basicConfig(filename=f"server{serverId}.log", level=logging.DEBUG)
    logging.info(f"Listening on {basePort + serverId}") 
    server = xmlrpc.server.SimpleXMLRPCServer(("localhost", basePort + serverId),
      allow_none=True, use_builtin_types=True)
    server.register_instance(KVSRPCServer())

    server.serve_forever()
