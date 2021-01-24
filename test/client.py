"""
Test the server
"""

import os, sys, logging
import pickle

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')


sys.path.append('../')
from protocol import Client
client = Client()
logging.info("Start set")
client.set('k0', 'v0')
logging.info("End set. Start get")
x = client.get('k0')
logging.info("End get")
print(f"expecting: v0 recieved:{x}")

client.set('k2', -0.345)
x = client.get('k2')
print(f"expecting: -0.345 recieved:{x}")

x = client.get('k1')
print(f"expecting: None recieved:{x}")

client.mset('k1', 'v1', 'k2', ['v2-0', 1, 'v2-2'], 'k3', 'v3')

client.get('k2')

client.mget('k3', 'k1')

client.delete('k1')

client.get('k1')
client.delete('k1')

client.set('kx', {'vx': {'vy': 0, 'vz': [1, 2.34, 3]}})

a = client.get('kx')
print(client.get('kx'))
print(f"expecting: 2.34 received: a['vx']['vz'][1]={a['vx']['vz'][1]}")

client.flush()
#

#print(client.blpop('list1'))

print(client.lpush('list1', 3.3))
print(client.lpush('list1', 5.634354234, 3, "abc", [3,5,6,7.7]))
print(f"expecting 5, getting {client.llen('list1')}")
print(client.rpop('list1'))
print(f"expecting 4, getting {client.llen('list1')}")
#print(client.get('list1'))

#print(client.blpop('newlist')) # need extra command to test block operations.

print(client.lpush('newlist', 77))

logging.info("Done")

# testing bytes
randomfile = os.urandom(10*1024*1024)
pfile = pickle.dumps(randomfile)
logging.info(f"About to write a big data, filesize={sys.getsizeof(randomfile)}, pickled size={sys.getsizeof(pfile)}")
client.set("file", pfile)
logging.info("done")

with open("copacabana.mp3", mode='rb') as file: # b is important -> binary
    fileContent = file.read()
    logging.info(f"size of file={sys.getsizeof(fileContent)}")
    client.set("mp3", fileContent)

logging.info("finished writting. start reading.")
copymp3 = client.get("mp3")
logging.info(f"get copy of mp3 size={sys.getsizeof(copymp3)}")
with open("a_copy.mp3", mode='wb') as file: # b is important -> binary
    file.write(copymp3)
logging.info("done writing the file.")

# Localhost: 20 minisecond to write 3M, 180 miniseconds to SET 5M, 30 miniseconds to GET 5M, 300 minisecond to write 10M;

with open("a.pdf", mode='rb') as file: # b is important -> binary
    fileContent = file.read()
    pfile = pickle.dumps(fileContent)
    logging.info(f"size of file={sys.getsizeof(fileContent)}, pickled size={sys.getsizeof(pfile)}")
    client.set("pdf", pfile)

logging.info("finished writting. start reading.")
pcopymp3 = client.get("pdf")
copymp3 = pickle.loads(pcopymp3)
logging.info(f"get copy of pdf size={sys.getsizeof(copymp3)}, unpickled from size of {sys.getsizeof(pcopymp3)}")
with open("a_copy.pdf", mode='wb') as file: # b is important -> binary
    file.write(copymp3)
logging.info("done writing the file from pickled format..")
