from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer

from collections import namedtuple
from io import BytesIO, StringIO
from socket import error as socket_error
import logging


logger = logging.getLogger(__name__)


class CommandError(Exception): pass
class Disconnect(Exception): pass

Error = namedtuple('Error', ('message',))


class ProtocolHandler(object):
    def __init__(self):
        self.handlers = {
            b'+': self.handle_simple_string,
            b'-': self.handle_error,
            b':': self.handle_integer,
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

    def handle_simple_string(self, socket_file):
        return socket_file.readline()

    def handle_error(self, socket_file):
        return Error(socket_file.readline())

    def handle_integer(self, socket_file):
        return int(socket_file.readline())

    def handle_string(self, socket_file):
        # First read the length ($<length>\r\n).
        length = int(socket_file.readline())

        if length == -1:
            return None  # Special-case for NULLs.
        length += 2  # Include the trailing \r\n in count.
        return socket_file.read(length)[:-2].decode("utf-8")

    def handle_array(self, socket_file):
        num_elements = int(socket_file.readline())
        #logger.warning(f"Received {num_elements} in handle_array")
        return [self.handle_request(socket_file) for _ in range(num_elements)]
    
    def handle_dict(self, socket_file):
        num_items = int(socket_file.readline().rstrip('\r\n'))
        elements = [self.handle_request(socket_file)
                    for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))

    def write_response(self, socket_file, data):
        buf = StringIO()
        self._write(buf, data)
        buf.seek(0)
        socket_file.write(buf.getvalue().encode("utf-8"))
        socket_file.flush()


    """Write the commands recursively, to a buffer (a BytesIO)."""
    def _write(self, buf, data):
        # if isinstance(data, str):
        #     data = data.encode('utf-8')

        if isinstance(data, bytes):
            buf.write(f'${len(data)}\r\n{data}\r\n')
        elif isinstance(data, str):
            buf.write(f'${len(data)}\r\n{data}\r\n')
        elif isinstance(data, int):
            buf.write(f':{data}\r\n' )
        elif isinstance(data, Error):
            buf.write(f'-{Error.message}\r\n' )
        elif isinstance(data, (list, tuple)):
            buf.write(f'*{len(data)}\r\n')
            for item in data:
                logger.info(f"Client send {item}")
                self._write(buf, item)
        elif isinstance(data, dict):
            buf.write(f'%%{len(data)}\r\n' )
            for key in data:
                self._write(buf, key)
                self._write(buf, data[key])
        elif data is None:
            buf.write('$-1\r\n')
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

        self._commands = self.get_commands()

    def get_commands(self):
        return {
            'GET': self.get,
            'SET': self.set,
            'DELETE': self.delete,
            'FLUSH': self.flush,
            'MGET': self.mget,
            'MSET': self.mset}

    def connection_handler(self, conn, address):
        logger.info('Connection received: %s:%s' % address)
        # Convert "conn" (a socket object) into a file-like object.
        socket_file = conn.makefile('rwb')

        # Process client requests until client disconnects.
        while True:
            try:
                data = self._protocol.handle_request(socket_file)
                logger.warning(f"In connection_handler() Received {len(data)} . The Data is {data}")
            except Disconnect:
                logger.info('Client went away: %s:%s' % address)
                break

            try:
                logger.info("Start get_response()")
                resp = self.get_response(data)
            except CommandError as exc:
                logger.exception('Command error')
                resp = Error(exc.args[0])

            self._protocol.write_response(socket_file, resp)

    def run(self):
        self._server.serve_forever()

    def get_response(self, data):
        logger.info(f"In Server.get_response, data is {data}")
        if not isinstance(data, list):
            try:
                data = data.split()
            except:
                raise CommandError('Request must be list or simple string.')

        if not data:
            raise CommandError('Missing command')

        command = data[0].upper()
        if command not in self._commands:
            raise CommandError('Unrecognized command: %s' % command)
        else:
            logger.debug('Received command %s', command)

        return self._commands[command](*data[1:])

    def get(self, key):
        logger.info(f" in get(), the _kv is {self._kv}. now trying to get {key}")
        return self._kv.get(key)

    def set(self, key, value):
        self._kv[key] = value
        logger.info(f" in set(), the _kv is {self._kv}")
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
        return len(data)


class Client(object):
    def __init__(self, host='127.0.0.1', port=31337):
        self._protocol = ProtocolHandler()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((host, port))
        self._fh = self._socket.makefile('rwb')

    def execute(self, *args):
        self._protocol.write_response(self._fh, args)
        resp = self._protocol.handle_request(self._fh)
        if isinstance(resp, Error):
            raise CommandError(resp.message)
        return resp

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


if __name__ == '__main__':
    from gevent import monkey; monkey.patch_all()
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)
    print("Start Server")
    s = Server()
    s.run()
    print("End Server")
