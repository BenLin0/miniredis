"""
Test the server
"""

import sys, logging
from protocol import Client, CommandError


logging.basicConfig(level=logging.WARNING, format='%(asctime)s [%(levelname)s] %(message)s')

client = Client()
print("Start miniredis client.")
print(client.info)
print("""Available commands:[GET],[SET],[DELETE],[FLUSH],[MGET],[MSET],[LPUSH],[RPUSH],[LPOP],[RPOP],[BLPOP],[BRPOP],[LLEN]
[EXPIRE],[TTL],[PERSIST]
([MULTI],[EXEC] to be complete)
[INFO],[QUIT],[EXIT]""")

while True:
    command = input(">")
    segments = command.split(" ")
    segments[0] = segments[0].upper()
    if segments[0] == "QUIT" or segments[0] == "EXIT":
        break
    arguments = tuple(segments)
    try:
        received = client.execute(*arguments)
        if isinstance(received, bytes):
            filename = "_".join(segments)+".bin"
            with open(filename, mode='wb') as file:  # b is important -> binary
                file.write(received)
            print(f"{sys.getsizeof(received)} bytes saved as :{filename}")
        else:
            print(received)
    except CommandError as e:
        print("Wrong error format. Please consult the manual.")
    except Exception as e:
        logging.error(e)
        print(f"Error when executing {command}")
