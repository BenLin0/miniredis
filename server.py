from protocol import Server
import logging
from gevent import monkey



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

    monkey.patch_all()
    s = Server()    # can be Server("127.0.0.1", 34567)
    print(s.help())
    s.run()
    print("End Server")
