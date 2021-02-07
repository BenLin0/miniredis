import threading, time, sys

from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer

from collections import namedtuple
from io import BytesIO
import logging


class CommandError(Exception): pass
class Disconnect(Exception): pass

Error = namedtuple('Error', ('message',))


class ProtocolHandler(object):
    def __init__(self):
        self.handlers = {
            b'+': self.handle_bytes,
            b'-': self.handle_error,
            b':': self.handle_integer,
            b'?': self.handle_float,
            b'$': self.handle_string,
            b'*': self.handle_array,
            b'%': self.handle_dict}

    def handle_request(self, socket_file):
        first_byte = socket_file.read(1)
        if not first_byte:
            raise Disconnect()
        #logging.warning(f"Received: {first_byte}")
        try:
            # Delegate to the appropriate handler based on the first byte.
            return self.handlers[first_byte](socket_file)
        except KeyError:
            raise CommandError('bad request')

    def handle_bytes(self, socket_file):
        length = int(socket_file.readline())
        length += 2  # Include the trailing \r\n in count.
        return socket_file.read(length)[:-2]

    def handle_error(self, socket_file):
        return Error(socket_file.readline())

    def handle_integer(self, socket_file):
        return int(socket_file.readline())

    def handle_float(self, socket_file):
        return float(socket_file.readline())

    def handle_string(self, socket_file):
        # First read the length ($<length>\r\n).
        length = int(socket_file.readline())

        if length == -1:
            return None  # Special-case for NULLs.
        length += 2  # Include the trailing \r\n in count.
        return socket_file.read(length)[:-2].decode("utf-8")

    def handle_array(self, socket_file):
        num_elements = int(socket_file.readline())
        #logging.warning(f"Received {num_elements} in handle_array")
        return [self.handle_request(socket_file) for _ in range(num_elements)]
    
    def handle_dict(self, socket_file):
        num_items = int(socket_file.readline())
        elements = [self.handle_request(socket_file)
                    for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))

    def write_response(self, socket_file, data):
        buf = BytesIO()
        self._write(buf, data)
        buf.seek(0)
        tosend = buf.getvalue()
        try:
            logging.debug(tosend.decode('utf-8'))
        except UnicodeDecodeError:
            logging.debug("contains byte info that can't be decode to utf-8")
            pass
        socket_file.write(tosend)
        socket_file.flush()


    """Write the commands recursively, to a buffer (a BytesIO)."""
    def _write(self, buf, data):
        # if isinstance(data, str):
        #     data = data.encode('utf-8')   # this is for python2. in python3, all data are in utf-8.

        if isinstance(data, bytes):
            buf.write(f'+{len(data)}\r\n'.encode('utf-8'))
            buf.write(data)
            buf.write("\r\n".encode('utf-8'))
        elif isinstance(data, str):
            buf.write(f'${len(data)}\r\n{data}\r\n'.encode('utf-8'))
        elif isinstance(data, int):
            buf.write(f':{data}\r\n'.encode('utf-8') )
        elif isinstance(data, float):
            buf.write(f'?{data}\r\n'.encode('utf-8'))
        elif isinstance(data, Error):
            buf.write(f'-{Error.message}\r\n'.encode('utf-8') )
        elif isinstance(data, (list, tuple)):
            buf.write(f'*{len(data)}\r\n'.encode('utf-8'))
            for item in data:
                self._write(buf, item)
        elif isinstance(data, dict):
            buf.write(f'%{len(data)}\r\n'.encode('utf-8') )
            for key in data:
                self._write(buf, key)
                self._write(buf, data[key])
        elif data is None:
            buf.write('$-1\r\n'.encode('utf-8'))
        else:
            raise CommandError('unrecognized type: %s' % type(data))


class Server(object):
    def __init__(self, host='127.0.0.1', port=31337, max_clients=64):
        self._pool = Pool(max_clients)
        self._server = StreamServer(
            (host, port),
            self.connection_handler,
            spawn=self._pool)

        self._protocol = ProtocolHandler()
        self._kv = {}
        self._queue = {}    # for callback.
        self._ttl = {}

        self._commands = self.get_commands()
        self.info = f"Server open in {host}:{port}"
        th = threading.Thread(target=self._checkttl)
        th.start()

    def help(self):
        output = f"{self.info}\r\n"
        output += "Available commands:" + ",".join([f"[{command}]" for command in self.get_commands()])
        return output

    def get_commands(self):
        return {
            'GET': self.get,
            'SET': self.set,
            'DELETE': self.delete,
            'FLUSH': self.flush,
            'MGET': self.mget,
            'MSET': self.mset,
            'LPUSH': self.lpush,
            'RPUSH': self.rpush,
            'LPOP': self.lpop,
            'RPOP': self.rpop,
            'BLPOP': self.blpop,
            'BRPOP': self.brpop,
            'LLEN': self.llen,
            'EXPIRE': self.expire,
            'TTL': self.ttl,
            'PERSIST': self.persist,
            'INFO': self.info
        }

    def connection_handler(self, conn, address):
        logging.info('Connection received from: %s:%s' % address)
        # Convert "conn" (a socket object) into a file-like object.
        socket_file = conn.makefile('rwb')

        # Process client requests until client disconnects.
        while True:
            try:
                data = self._protocol.handle_request(socket_file)
                logging.debug(f"In Server.connection_handler() Server Received {len(data)} . The Data is {data}")
            except Disconnect:
                logging.info('Client went away: %s:%s' % address)
                break

            try:
                resp = self.get_response(data)
            except CommandError as exc:
                logging.exception('Command error')
                resp = Error(exc.args[0])
            except TypeError as err:
                resp = Error(f"Wrong format. {err}")
            except Exception as err:
                resp = Error(f"Unknown error. {err}")

            self._protocol.write_response(socket_file, resp)

    def run(self):
        self._server.serve_forever()

    def get_response(self, data):
        if not isinstance(data, list):
            try:
                data = data.split()
            except:
                raise CommandError('Request must be list or simple string.')

        if not data:
            raise CommandError('Missing command')

        command = data[0]
        if command not in self._commands:
            raise CommandError('Unrecognized command: %s' % command)

        return self._commands[command](*data[1:])

    #Below are REDIS commands in server.
    def get(self, key):
        return self._kv.get(key)

    def set(self, key, value):
        self._kv[key] = value
        #print(f"SET: {key}, {type(self._kv[key])}, {sys.getsizeof(self._kv[key])} value={value}\n")
        if key in self._queue:
            callback = self._queue.pop(0)
            callback()
        return 1

    def delete(self, key):
        if key in self._kv:
            del self._kv[key]
            return 1
        return 0

    def flush(self):
        kvlen = len(self._kv)
        self._kv.clear()
        return kvlen

    def mget(self, *keys):
        return [self._kv.get(key) for key in keys]

    def mset(self, *items):
        data = zip(items[::2], items[1::2])
        for key, value in data:
            self._kv[key] = value
        return len(list(data))

    def releaseblock(self, key):
        if key in self._queue and self._queue[key]:
            releaseblockevent = self._queue[key].pop()
            while releaseblockevent:
                if releaseblockevent.is_set():
                       # this event is already timeout. no one is waiting for it. ignore. pop the next.
                    if self._queue[key]:
                        releaseblockevent = self._queue[key].pop()
                    else:   # the _queue[key] is empty, then ignore.
                        break
                else:
                    releaseblockevent.set()
                    break

    def lpush(self, *items):
        key = items[0]
        if key not in self._kv:
            self._kv[key] = []
        if not isinstance(self._kv[key], list):
            logging.warning(f"Element {key} is not a list")
            return None
        for value in items[1:]:
            self._kv[key].insert(0, value)

        self.releaseblock(key)
        return len(self._kv[key])

    def rpush(self, *items):
        key = items[0]
        if key not in self._kv:
            self._kv[key] = []
        if not isinstance(self._kv[key], list):
            logging.warning(f"Element {key} is not a list")
            return None
        for value in items[1:]:
            self._kv[key].append(value)

        self.releaseblock(key)
        return len(self._kv[key])

    def lpop(self, key):
        if key not in self._kv:
            return None
        if not isinstance(self._kv[key], list):
            logging.warning(f"Element {key} is not a list")
            return None
        try:
            value = self._kv[key].pop(0)
        except IndexError:
            return None
        return value

    def rpop(self, key):
        if key not in self._kv:
            return None
        if not isinstance(self._kv[key], list):
            logging.warning(f"Element {key} is not a list")
            return None
        try:
            value = self._kv[key].pop(-1)
        except IndexError:
            return None
        return value

    def llen(self, key):
        if key not in self._kv:
            return None
        return len(self._kv[key])

    def blpop(self, key, timeout=60):
        if key not in self._kv:
            self._kv[key] = []  #  even if this key doesn't exist, block pop still wait for this list.

        value = self.lpop(key)
        if value:
            return value

        waittogetvalue = threading.Event()
        if key not in self._queue:
            self._queue[key] = []
        self._queue[key].append(waittogetvalue)

        i = waittogetvalue.wait(timeout)
        if waittogetvalue.is_set():
            value = self.lpop(key)
            if value:
                return value
            else:
                logging.error("blpop(). The logic here should be able to retrieve a value. might need an atomic execution around here.")
                return None
        else:
            waittogetvalue.set()    # set it to abandon it, so the releaseblock() function will move on to next block in _queue
            return None

    def brpop(self, key, timeout=30):
        if key not in self._kv:
            self._kv[key] = []

        value = self.rpop(key)
        if value:
            return value

        waittogetvalue = threading.Event()
        if key not in self._queue:
            self._queue[key] = []
        self._queue[key].append(waittogetvalue)

        i = waittogetvalue.wait(timeout)
        if waittogetvalue.is_set():
            value = self.rpop(key)
            if value:
                return value
            else:
                logging.error("brpop(). The logic here should be able to retrieve a value. might need an atomic execution around here.")
                return None
        else:
            waittogetvalue.set()    # set it to abandon it, so the releaseblock() function will move on to next block in _queue
            return None

    def _checkttl(self):    #
        while(True):
            todelete = []
            for key in self._ttl:
                self._ttl[key] -= 1
                if self._ttl[key] < 0:
                    todelete.append(key)

            for key in todelete:
                del self._ttl[key]
                del self._kv[key]

            time.sleep(60)

    def expire(self, key, timeout):
        self._ttl[key] = int(timeout)
        return timeout

    def persist(self, key):
        if key in self._ttl:
            del self._ttl[key]
        return None

    def ttl(self, key):
        if key not in self._ttl:
            return -1
        return self._ttl[key]

    def info(self):
        result = "key, type, size\n"
        for k in sorted(self._kv):
            result += f"{k}, {type(self._kv[k])}, {sys.getsizeof(self._kv[k])}\n"
        return result


class Client(object):
    def __init__(self, host='127.0.0.1', port=31337, poolnum=2):
        self._protocol = ProtocolHandler()
        self.lock = threading.RLock()
        self._fh = []
        for i in range(poolnum):
            _socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            _socket.connect((host, port))
            self._fh.append(_socket.makefile('rwb'))
        self.info = f"Connected to {host}:{port}"

    def close(self):
        pass
        # self._socket.close()

    def execute(self, *args):
        self.lock.acquire()
        try:
            self._protocol.write_response(self._fh[0], args)
            resp = self._protocol.handle_request(self._fh[0])
        finally:
            self.lock.release()
        if isinstance(resp, Error):
            raise CommandError(resp.message)
        return resp

    # Below are REDIS commands in client.
    def get(self, key):
        return self.execute('GET', key)

    def set(self, key, value):
        return self.execute('SET', key, value)

    def delete(self, key):
        return self.execute('DELETE', key)

    def flush(self):
        return self.execute('FLUSH')

    def mget(self, *keys):
        return self.execute('MGET', *keys)

    def mset(self, *items):
        return self.execute('MSET', *items)

    def lpush(self, *items):
        return self.execute('LPUSH', *items)

    def rpush(self, *items):
        return self.execute('RPUSH', *items)

    def lpop(self, key):
        return self.execute('LPOP', key)

    def rpop(self, key):
        return self.execute('RPOP', key)

    def llen(self, key):
        return self.execute('LLEN', key)

    def blpop(self, key):
        return self.execute('BLPOP', key)

    def brpop(self, key):
        return self.execute('BRPOP', key)


