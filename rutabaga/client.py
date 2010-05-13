# -*- coding: utf -*-
import socket
from tornado.ioloop import IOLoop
from tornado.iostream import IOStream
import adisp
from functools import partial
from collections import namedtuple
from rutabaga.exceptions import RedisError, ConnectionError, ResponseError, InvalidResponse


Message = namedtuple('Message', 'kind channel body')


class Task(object):
    def __init__(self, command, callbacks, command_args, command_kwargs):
        self.command = command
        self.command_args = command_args
        self.command_kwargs = command_kwargs
        self.callbacks = callbacks


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
        string_keys_to_dict('FLUSHALL FLUSHDB SELECT SET',
                            lambda r: r == 'OK'),
        string_keys_to_dict('HGETALL',
                            lambda pairs: dict(zip(pairs[::2], pairs[1::2]))),
        string_keys_to_dict('GET SUBSTR',
                            str),
        string_keys_to_dict('HGET',
                            lambda r: r or ''),
        string_keys_to_dict('SUBSCRIBE UNSUBSCRIBE LISTEN',
                            lambda r: Message(*r)),
        )


    def __init__(self, host='localhost', port=6379, io_loop=None):
        self.connection = Connection(host, port, io_loop=io_loop)
        self.queue = []
        self.in_progress = False
        self.current_task = None
        self.subscribed = False

    def __repr__(self):
        return 'Rutabaga client (host=%s, port=%s)' % (self.connection.host, self.connection.port)

    def connect(self):
        self.connection.connect()

    def disconnect(self):
        self.connection.disconnect()

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
            self.call_callbacks(self.current_task.callbacks, None, error)
        else:
            self.call_callbacks(self.current_task.callbacks, self.format_reply(self.current_task.command, data), None)
        self.in_progress = False
        self.try_to_loop()

    def call_callbacks(self, callbacks, *args, **kwargs):
        for cb in callbacks:
            cb(*args, **kwargs)

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

    def schedule(self, command, callbacks, *args, **kwargs):
        self.queue.append(Task(command, callbacks, args, kwargs))

    def do_multibulk(self, length):
        tokens = []
        def on_data(data, error):
            if error:
                self.propogate_result(None, error)
                return
            tokens.append(data)
        [ self._process_response([on_data]) for i in xrange(length) ]
        self.propogate_result(tokens, None)

    @adisp.process
    def _process_response(self, callbacks=None):
        callbacks = callbacks or [self.propogate_result]
        data = yield adisp.async(self.connection.readline)()
        #print 'd:', data
        if not data:
            self.connection.disconnect()
            self.call_callbacks(callbacks, None, ConnectionError("Socket closed on remote end"))
            return
        if data in ('$-1', '*-1'):
            callback(None, None)
            return
        head, tail = data[0], data[1:]
        if head == '-':
            if tail.startswith('ERR '):
                tail = tail[4:]
            self.call_callbacks(callbacks, None, ResponseError(self.current_task, tail))
        elif head == '+':
            self.call_callbacks(callbacks, tail, None)
        elif head == ':':
            self.call_callbacks(callbacks, int(tail), None)
        elif head == '$':
            length = int(tail)
            if length == -1:
                callback(None)
            data = yield adisp.async(self.connection.read)(length+2)
            data = data[:-2] # strip \r\n
            self.call_callbacks(callbacks, data, None)
        elif head == '*':
            length = int(tail)
            if length == -1:
                self.call_callbacks(callbacks, None, None)
            else:
                self.do_multibulk(length)
        else:
            self.call_callbacks(callbacks, None, InvalidResponse("Unknown response type for: %s" % self.current_task.command))

    def execute_command(self, cmd, callbacks, *args, **kwargs):
        if callbacks is None:
            callbacks = []
        elif not hasattr(callbacks, '__iter__'):
            callbacks = [callbacks]
        self.connection.write(self.format(cmd, *args, **kwargs))
        self.schedule(cmd, callbacks, *args, **kwargs)
        self.try_to_loop()


    ### MAINTENANCE
    def flushall(self, *callbacks):
        self.execute_command('FLUSHALL', callbacks)

    def flushdb(self, *callbacks):
        self.execute_command('FLUSHDB', callbacks)

    def dbsize(self, *callbacks):
        self.execute_command('DBSIZE', callbacks)

    def select(self, db, *callbacks):
        self.execute_command('SELECT', callbacks, db)

    ### BASIC KEY COMMANDS
    def append(self, key, value, *callbacks):
        self.execute_command('APPEND', callbacks, key, value)

    def substr(self, key, start, end, *callback):
        self.execute_command('SUBSTR', callbacks, key, start, end)

    def delete(self, key, *callback):
        self.execute_command('DEL', callbacks, key)

    def set(self, key, value, *callbacks):
        self.execute_command('SET', callbacks, key, value)

    def get(self, key, *callbacks):
        self.execute_command('GET', callbacks, key)

    ### HASH COMMANDS
    def hgetall(self, key, *callbacks):
        self.execute_command('HGETALL', callbacks, key)

    def hmset(self, key, mapping, *callbacks):
        items = []
        [ items.extend(pair) for pair in mapping.iteritems() ]
        self.execute_command('HMSET', callbacks, key, *items)

    def hset(self, key, hkey, value, *callbacks):
        self.execute_command('HSET', callbacks, key, hkey, value)

    def hget(self, key, hkey, *callbacks):
        self.execute_command('HGET', callbacks, key, hkey)

    def hdel(self, key, hkey, *callbacks):
        self.execute_command('HDEL', callbacks, key, hkey)

    def hlen(self, key, *callbacks):
        self.execute_command('HLEN', callbacks, key)

    ### PUBSUB
    def subscribe(self, channels, *callbacks):
        if isinstance(channels, basestring):
            channels = [channels]
        callbacks = list(callbacks) + [self.on_subscribed]
        self.execute_command('SUBSCRIBE', callbacks, *channels)

    def on_subscribed(self, _r, e):
        if not e:
            self.subscribed = True

    def unsubscribe(self, channels, *callbacks):
        if isinstance(channels, basestring):
            channels = [channels]
        callbacks = list(callbacks) + [self.on_unsubscribed]
        self.execute_command('UNSUBSCRIBE', callbacks, *channels)

    def on_unsubscribed(self, _r, e):
        if not e:
            self.subscribed = False

    def publish(self, channel, message, *callbacks):
        self.execute_command('PUBLISH', callbacks, channel, message)

    def listen(self, *callbacks):
        # 'LISTEN' is just for exception information, it is not actually sent anywhere
        if self.on_message not in callbacks:
            callbacks = list(callbacks) + [self.on_message]
        self.schedule('LISTEN', callbacks)
        self.try_to_loop()

    def on_message(self, _data, _error):
        if self.subscribed:
            self.schedule('LISTEN', self.current_task.callbacks)
            self.try_to_loop()

