"""
Test the server
"""

import sys, logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')


sys.path.append('../')
from protocol import Client, CommandError
client = Client()
print("Start miniredis client.")
print(client.info)
print("Available commands:[GET],[SET],[DELETE],[FLUSH],[MGET],[MSET],[LPUSH],[RPUSH],[LPOP],[RPOP],[BLPOP],[BRPOP],[LLEN],[MULTI],[EXEC],[QUIT],[EXIT]")
client.set('k0', 'v0')      # for debugging.
while True:
    command = input(">")
    segments = command.split(" ")
    segments[0] = segments[0].upper()
    if segments[0] == "QUIT" or segments[0] == "EXIT":
        break
    arguments = tuple(segments)
    try:
        print(client.execute(*arguments))
    except CommandError as e:
        print("Wrong error format. Please consult the manual.")
    except Exception as e:
        logging.error(e)
        print(f"Error when executing {command}")
