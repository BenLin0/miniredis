"""
Test the server
"""

import sys, logging

sys.path.append('../')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

from server_example import Client
client = Client()
client.set('k0', 'v0')
x = client.get('k0')
print(f"recieved:{x}")

client.set('k2', 'v2')
x = client.get('k2')
print(f"recieved:{x}")

x = client.get('k1')
print(f"recieved:{x}")
#
# client.mset('k1', 'v1', 'k2', ['v2-0', 1, 'v2-2'], 'k3', 'v3')
#
# client.get('k2')
#
# client.mget('k3', 'k1')
#
# client.delete('k1')
#
# client.get('k1')
# client.delete('k1')
#
# client.set('kx', {'vx': {'vy': 0, 'vz': [1, 2, 3]}})
#
# client.get('kx')
#
# client.flush()
#
