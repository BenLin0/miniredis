"""
Test the server
"""

import sys, logging

sys.path.append('../')
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

logger = logging.getLogger(__name__)


from server_example import Client
client = Client()
logger.info("Start set")
client.set('k0', 'v0')
logger.info("End set. Start get")
x = client.get('k0')
logger.info("End get")
print(f"recieved:{x}")

client.set('k2', 'v2')
x = client.get('k2')
print(f"recieved:{x}")

x = client.get('k1')
print(f"recieved:{x}")

client.mset('k1', 'v1', 'k2', ['v2-0', 1, 'v2-2'], 'k3', 'v3')

client.get('k2')

client.mget('k3', 'k1')

client.delete('k1')

client.get('k1')
client.delete('k1')

client.set('kx', {'vx': {'vy': 0, 'vz': [1, 2.34, 3]}})

a = client.get('kx')
print(client.get('kx'))
print(f"a['vx']['vz'][1]={a['vx']['vz'][1]}")

# client.flush()
#

print(client.lpop('list1'))

print(client.lpush('list1', 3.3))
print(client.lpush('list1', 5.634354234, 3, "abc", [3,5,6,7.7]))
print(client.llen('list1'))
print(client.rpop('list1'))
print(client.llen('list1'))


logger.info("Done")