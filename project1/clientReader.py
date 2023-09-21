import argparse
import xmlrpc.client
import xmlrpc.server
import logging

frontend = xmlrpc.client.ServerProxy("http://localhost:8001")

if __name__ == '__main__':
  print(frontend.get("key"))

