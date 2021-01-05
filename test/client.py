"""
Test the server
"""

import sys, logging


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

# client.flush()
#

print(client.lpop('list1'))

print(client.lpush('list1', 3.3))
print(client.lpush('list1', 5.634354234, 3, "abc", [3,5,6,7.7]))
print(client.llen('list1'))
print(client.rpop('list1'))
print(client.llen('list1'))
#print(client.get('list1'))


logging.info("Done")