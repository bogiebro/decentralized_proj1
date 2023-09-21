import argparse
import xmlrpc.client
import xmlrpc.server
import logging

frontend = xmlrpc.client.ServerProxy("http://localhost:8001")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', nargs=1, type=int, dest='val', required=True)
    args = parser.parse_args()
    clientId = args.val[0]
    print(frontend.put("key", args.val[0]))
