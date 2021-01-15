from protocol import Server
import logging


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

    print("Start Server")
    s = Server()    # can be Server("127.0.0.1", 34567)
    print(s.help())
    s.run()
    print("End Server")
