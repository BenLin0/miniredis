"""
Test the server
"""
import concurrent.futures
import sys, logging
import argparse
import multiprocessing
import time

from protocol import Client, CommandError

globalclient = Client()

class Process(multiprocessing.Process):
    def __init__(self):
        super(Process, self).__init__()


    def run(self):
        globalclient.set('foo', 'bar')

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", help="commands to test")
    parser.add_argument("-r", help="key space")
    parser.add_argument("-n", help='repeat', default=50)
    args = parser.parse_args()



    p = Process()
    p.start()
    p.join()

    logging.info("Start")
    starttime = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        f = [executor.submit(globalclient.set, "foo", "bar") for i in range(int(args.n))]
        concurrent.futures.wait(f)

    endtime = time.time()
    logging.info("Done!")
    print(f"""======SET====
    {args.n} requests completed in {endtime-starttime} seconds
    50 parallel clients

    """)


