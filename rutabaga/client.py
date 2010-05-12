# -*- coding: utf -*-
import socket
from tornado.ioloop import IOLoop
from tornado.iostream import IOStream
import adisp
from functools import partial
from collections import namedtuple
from rutabaga.exceptions import RedisError, ConnectionError, ResponseError, InvalidResponse


NOOP_CB = lambda _result, _error: None

Task = namedtuple('Task', 'command callback args kwargs')


def string_keys_to_dict(key_string, callback):
    return dict([(key, callback) for key in key_string.split()])

def dict_merge(*dicts):
    merged = {}
    [merged.update(d) for d in dicts]
    return merged


class Connection(object):
    def __init__(self, host, port, timeout=None, io_loop=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._stream = None
        self._io_loop = io_loop

    def connect(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
            sock.connect((self.host, self.port))
            self._stream = IOStream(sock, io_loop=self._io_loop)
        except socket.error, e:
            raise ConnectionError(str(e))

    def disconnect(self):
        try:
            self._stream.close()
        except socket.error, e:
            pass
        self._stream = None

    def write(self, data):
        self._stream.write(data)

    def consume(self, length):
        self._stream.read_bytes(length, NOOP_CB)

    def read(self, length, callback):
        self._stream.read_bytes(length, callback)

    def readline(self, callback):
        self._stream.read_until('\r\n', lambda data: callback(data[:-2]))


class Client(object):
    REPLY_MAP = dict_merge(
        string_keys_to_dict('DEL EXISTS HDEL HEXISTS HMSET',
                            bool),
        string_keys_to_dict('APPEND DBSIZE HLEN',
                            int),
        string_keys_to_dict('FLUSHALL FLUSHDB SET',
                            lambda r: r == 'OK'),
        string_keys_to_dict('HGETALL',
                            lambda pairs: dict(zip(pairs[::2], pairs[1::2]))),
        string_keys_to_dict('GET SUBSTR',
                            str),
        string_keys_to_dict('HGET',
                            lambda r: r or ''),
        )


    def __init__(self, host='localhost', port=6379, io_loop=None):
        self.connection = Connection(host, port, io_loop=io_loop)
        self.queue = []
        self.in_progress = False
        self.current_task = None

    def __repr__(self):
        return 'Rutabaga client (host=%s, port=%s)' % (self.connection.host, self.connection.port)

    def encode(self, value):
        if isinstance(value, str):
            return value
        elif isinstance(value, unicode):
            return value.encode(self.encoding, 'utf-8')
        # pray and hope
        return str(value)

    def format(self, *tokens):
        cmds = []
        for t in tokens:
            e_t = self.encode(t)
            cmds.append('$%s\r\n%s\r\n' % (len(e_t), e_t))
        return '*%s\r\n%s' % (len(tokens), ''.join(cmds))

    def propogate_result(self, data, error):
        if error:
            self.current_task.callback(None, error)
        else:
            self.current_task.callback(self.format_reply(self.current_task.command, data), None)
        self.in_progress = False
        self.try_to_loop()

    def format_reply(self, command, data):
        if command not in Client.REPLY_MAP:
            return data
        return Client.REPLY_MAP[command](data)

    def try_to_loop(self):
        if not self.in_progress and self.queue:
            self.in_progress = True
            self.current_task = self.queue.pop(0)
            self._process_response()
        elif not self.queue:
            self.current_task = None

    def schedule(self, command, callback, *args, **kwargs):
        self.queue.append(Task(command, callback, args, kwargs))

    def do_multibulk(self, length):
        tokens = []
        def on_data(data, error):
            if error:
                self.propogate_result(None, error)
                return
            tokens.append(data)
        [ self._process_response(on_data) for i in xrange(length) ]
        self.propogate_result(tokens, None)

    @adisp.process
    def _process_response(self, callback=None):
        callback = callback or self.propogate_result
        data = yield adisp.async(self.connection.readline)()
        #print 'd:', data
        if not data:
            self.connection.disconnect()
            callback(None, ConnectionError("Socket closed on remote end"))
            return
        if data in ('$-1', '*-1'):
            callback(None, None)
            return
        head, tail = data[0], data[1:]
        if head == '-':
            if tail.startswith('ERR '):
                tail = tail[4:]
            callback(None, ResponseError(self.current_task, tail))
        elif head == '+':
            callback(tail, None)
        elif head == ':':
            callback(int(tail), None)
        elif head == '$':
            length = int(tail)
            if length == -1:
                callback(None)
            data = yield adisp.async(self.connection.read)(length+2)
            data = data[:-2] # strip \r\n
            callback(data, None)
        elif head == '*':
            length = int(tail)
            if length == -1:
                callback(None, None)
            else:
                self.do_multibulk(length)
        else:
            callback(None, InvalidResponse("Unknown response type for: %s" % self.curr_command))

    def execute_command(self, cmd, callback, *args, **kwargs):
        self.connection.write(self.format(cmd, *args, **kwargs))
        self.schedule(cmd, callback, *args, **kwargs)
        self.try_to_loop()


    ### MAINTENANCE
    def flushdb(self, callback=NOOP_CB):
        self.execute_command('FLUSHDB', callback)

    def dbsize(self, callback=NOOP_CB):
        self.execute_command('DBSIZE', callback)

    ### BASIC KEY COMMANDS
    def append(self, key, value, callback=NOOP_CB):
        self.execute_command('APPEND', callback, key, value)

    def substr(self, key, start, end, callback=NOOP_CB):
        self.execute_command('SUBSTR', callback, key, start, end)

    def delete(self, key, callback=NOOP_CB):
        self.execute_command('DEL', callback, key)

    def set(self, key, value, callback=NOOP_CB):
        self.execute_command('SET', callback, key, value)

    def get(self, key, callback=NOOP_CB):
        self.execute_command('GET', callback, key)

    ### HASH COMMANDS

    def hgetall(self, key, callback=NOOP_CB):
        self.execute_command('HGETALL', callback, key)

    def hmset(self, key, mapping, callback=NOOP_CB):
        items = []
        [ items.extend(pair) for pair in mapping.iteritems() ]
        return self.execute_command('HMSET', callback, key, *items)

    def hset(self, key, hkey, value, callback=NOOP_CB):
        return self.execute_command('HSET', callback, key, hkey, value)

    def hget(self, key, hkey, callback=NOOP_CB):
        return self.execute_command('HGET', callback, key, hkey)

    def hdel(self, key, hkey, callback=NOOP_CB):
        return self.execute_command('HDEL', callback, key, hkey)

    def hlen(self, key, callback=NOOP_CB):
        return self.execute_command('HLEN', callback, key)


