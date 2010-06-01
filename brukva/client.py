# -*- coding: utf -*-
import socket
from tornado.ioloop import IOLoop
from tornado.iostream import IOStream
import adisp
from functools import partial
from collections import namedtuple
from datetime import datetime
from brukva.exceptions import RedisError, ConnectionError, ResponseError, InvalidResponse


Message = namedtuple('Message', 'kind channel body')
Task = namedtuple('Task', 'command callbacks command_args command_kwargs')


def string_keys_to_dict(key_string, callback):
    return dict([(key, callback) for key in key_string.split()])

def dict_merge(*dicts):
    merged = {}
    [merged.update(d) for d in dicts]
    return merged

def parse_info(response):
    info = {}
    def get_value(value):
        if ',' not in value:
            return value
        sub_dict = {}
        for item in value.split(','):
            k, v = item.split('=')
            try:
                sub_dict[k] = int(v)
            except ValueError:
                sub_dict[k] = v
        return sub_dict
    for line in response.splitlines():
        key, value = line.split(':')
        try:
            info[key] = int(value)
        except ValueError:
            info[key] = get_value(value)
    return info


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
            sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
            sock.settimeout(self.timeout)
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
        self._stream.read_until('\r\n', callback)


class Client(object):
    def __init__(self, host='localhost', port=6379, io_loop=None):
        self._io_loop = io_loop or IOLoop.instance()
        self.connection = Connection(host, port, io_loop=self._io_loop)
        self.queue = []
        self.in_progress = False
        self.current_task = None
        self.subscribed = False
        self.REPLY_MAP = dict_merge(
                string_keys_to_dict('BGREWRITEAOF BGSAVE DEL EXISTS EXPIRE HDEL HEXISTS '
                                    'HMSET MOVE MSET MSETNX SAVE SETNX',
                                    bool),
                string_keys_to_dict('FLUSHALL FLUSHDB SELECT SET SETEX SHUTDOWN '
                                    'RENAME RENAMENX',
                                    lambda r: r == 'OK'),
                string_keys_to_dict('SMEMBERS SINTER SUNION SDIFF',
                                    set),
                string_keys_to_dict('HGETALL',
                                    lambda pairs: dict(zip(pairs[::2], pairs[1::2]))),
                string_keys_to_dict('HGET',
                                    lambda r: r or ''),
                string_keys_to_dict('SUBSCRIBE UNSUBSCRIBE LISTEN',
                                    lambda r: Message(*r)),
                string_keys_to_dict('ZRANK ZREVRANK',
                                    lambda r: int(r) if r is not None else None),
                string_keys_to_dict('ZSCORE ZINCRBY',
                                    lambda r: float(r) if r is not None else None),
                string_keys_to_dict('ZRANGE ZRANGEBYSCORE ZREVRANGE',
                                    self.zset_score_pairs),
                {'PING': lambda r: r == 'PONG'},
                {'LASTSAVE': lambda t: datetime.fromtimestamp(int(t))},
                {'TTL': lambda r: r != -1 and r or None},
                {'INFO': parse_info},
            )

    def zset_score_pairs(self, response):
        if not response or not 'WITHSCORES' in self.current_task.command_args:
            return response
        return zip(response[::2], map(float, response[1::2]))

    def __repr__(self):
        return 'Brukva client (host=%s, port=%s)' % (self.connection.host, self.connection.port)

    def connect(self):
        self.connection.connect()

    def disconnect(self):
        self.connection.disconnect()

    def encode(self, value):
        if isinstance(value, str):
            return value
        elif isinstance(value, unicode):
            return value.encode('utf-8')
        # pray and hope
        return str(value)

    def format(self, *tokens):
        cmds = []
        for t in tokens:
            e_t = self.encode(t)
            cmds.append('$%s\r\n%s\r\n' % (len(e_t), e_t))
        return '*%s\r\n%s' % (len(tokens), ''.join(cmds))

    def propogate_result(self, result):
        (error, data) = result
        if error:
            self.call_callbacks(self.current_task.callbacks, (error, None))
        else:
            self.call_callbacks(self.current_task.callbacks, (None, self.format_reply(self.current_task.command, data)))
        self.in_progress = False
        self.try_to_loop()

    def call_callbacks(self, callbacks, *args, **kwargs):
        for cb in callbacks:
            cb(*args, **kwargs)

    def format_reply(self, command, data):
        if command not in self.REPLY_MAP:
            return data
        return self.REPLY_MAP[command](data)

    def try_to_loop(self):
        if not self.in_progress and self.queue:
            self.in_progress = True
            self.current_task = self.queue.pop(0)
            self._io_loop.add_callback(self._process_response)
        elif not self.queue:
            self.current_task = None

    def schedule(self, command, callbacks, *args, **kwargs):
        self.queue.append(Task(command, callbacks, args, kwargs))

    def _sudden_disconnect(self, callbacks):
        self.connection.disconnect()
        self.call_callbacks(callbacks, (ConnectionError("Socket closed on remote end"), None))

    def do_multibulk(self, length):
        tokens = []
        def on_data(result):
            (error, data) = result
            if error:
                self.propogate_result((error, None))
                return
            tokens.append(data)
            if len(tokens) == length:
                self.propogate_result((None, tokens))
            else:
                self._io_loop.add_callback(read_more)
        read_more = partial(self._process_response, [on_data])
        self._io_loop.add_callback(read_more)

    def _process_response(self, callbacks=None):
        callbacks = callbacks or [self.propogate_result]
        self.connection.readline(partial(self._parse_command_response, callbacks))

    def _parse_value_response(self, callbacks, data):
        if not data:
            self._sudden_disconnect(callbacks)
            return
        data = data[:-2] # strip \r\n
        self.call_callbacks(callbacks, (None, data))

    def _parse_command_response(self, callbacks, data):
        if not data:
            self._sudden_disconnect(callbacks)
            return
        data = data[:-2] # strip \r\n
        if data == '$-1':
            self.call_callbacks(callbacks, (None, None))
            return
        elif data == '*0' or data == '*-1':
            self.call_callbacks(callbacks, (None, []))
            return
        head, tail = data[0], data[1:]
        if head == '*':
            self.do_multibulk(int(tail))
        elif head == '$':
            self.connection.read(int(tail)+2, partial(self._parse_value_response, callbacks))
        elif head == '+':
            self.call_callbacks(callbacks, (None, tail))
        elif head == ':':
            self.call_callbacks(callbacks, (None, int(tail)))
        elif head == '-':
            if tail.startswith('ERR '):
                tail = tail[4:]
            self.call_callbacks(callbacks, (ResponseError(self.current_task, tail), None))
        else:
            self.call_callbacks(callbacks, (InvalidResponse("Unknown response type for: %s" % self.current_task.command), None))

    def execute_command(self, cmd, callbacks, *args, **kwargs):
        if callbacks is None:
            callbacks = []
        elif not hasattr(callbacks, '__iter__'):
            callbacks = [callbacks]
        try:
            self.connection.write(self.format(cmd, *args, **kwargs))
        except IOError:
            self._sudden_disconnect(callbacks)
            return
        self.schedule(cmd, callbacks, *args, **kwargs)
        self.try_to_loop()


    ### MAINTENANCE
    def bgrewriteaof(self, callbacks=None):
        self.execute_command('BGREWRITEAOF', callbacks)

    def dbsize(self, callbacks=None):
        self.execute_command('DBSIZE', callbacks)

    def flushall(self, callbacks=None):
        self.execute_command('FLUSHALL', callbacks)

    def flushdb(self, callbacks=None):
        self.execute_command('FLUSHDB', callbacks)

    def ping(self, callbacks=None):
        self.execute_command('PING', callbacks)

    def info(self, callbacks=None):
        self.execute_command('INFO', callbacks)

    def select(self, db, callbacks=None):
        self.execute_command('SELECT', callbacks, db)

    def shutdown(self, callbacks=None):
        self.execute_command('SHUTDOWN', callbacks)

    def save(self, callbacks=None):
        self.execute_command('SAVE', callbacks)

    def bgsave(self, callbacks=None):
        self.execute_command('BGSAVE', callbacks)

    def lastsave(self, callbacks=None):
        self.execute_command('LASTSAVE', callbacks)

    def keys(self, pattern, callbacks=None):
        self.execute_command('KEYS', callbacks, pattern)

    ### BASIC KEY COMMANDS
    def append(self, key, value, callbacks=None):
        self.execute_command('APPEND', callbacks, key, value)

    def expire(self, key, ttl, callbacks=None):
        self.execute_command('EXPIRE', callbacks, key, ttl)

    def ttl(self, key, callbacks=None):
        self.execute_command('TTL', callbacks, key)

    def type(self, key, callbacks=None):
        self.execute_command('TYPE', callbacks, key)

    def randomkey(self, callbacks=None):
        self.execute_command('RANDOMKEY', callbacks)

    def rename(self, src, dst, callbacks=None):
        self.execute_command('RENAME', callbacks, src, dst)

    def renamenx(self, src, dst, callbacks=None):
        self.execute_command('RENAMENX', callbacks, src, dst)

    def move(self, key, db, callbacks=None):
        self.execute_command('MOVE', callbacks, key, db)

    def substr(self, key, start, end, callbacks=None):
        self.execute_command('SUBSTR', callbacks, key, start, end)

    def delete(self, key, callbacks=None):
        self.execute_command('DEL', callbacks, key)

    def set(self, key, value, callbacks=None):
        self.execute_command('SET', callbacks, key, value)

    def setex(self, key, ttl, value, callbacks=None):
        self.execute_command('SETEX', callbacks, key, ttl, value)

    def setnx(self, key, value, callbacks=None):
        self.execute_command('SETNX', callbacks, key, value)

    def mset(self, mapping, callbacks=None):
        items = []
        [ items.extend(pair) for pair in mapping.iteritems() ]
        self.execute_command('MSET', callbacks, *items)

    def msetnx(self, mapping, callbacks=None):
        items = []
        [ items.extend(pair) for pair in mapping.iteritems() ]
        self.execute_command('MSETNX', callbacks, *items)

    def get(self, key, callbacks=None):
        self.execute_command('GET', callbacks, key)

    def mget(self, keys, callbacks=None):
        self.execute_command('MGET', callbacks, *keys)

    def getset(self, key, value, callbacks=None):
        self.execute_command('GETSET', callbacks, key, value)

    def exists(self, key, callbacks=None):
        self.execute_command('EXISTS', callbacks, key)

    ### COUNTERS COMMANDS
    def incr(self, key, callbacks=None):
        self.execute_command('INCR', callbacks, key)

    def decr(self, key, callbacks=None):
        self.execute_command('DECR', callbacks, key)

    def incrby(self, key, amount, callbacks=None):
        self.execute_command('INCRBY', callbacks, key, amount)

    def decrby(self, key, amount, callbacks=None):
        self.execute_command('DECRBY', callbacks, key, amount)

    ### LIST COMMANDS
    def blpop(self, keys, timeout=0, callbacks=None):
        tokens = list(keys)
        tokens.append(timeout)
        self.execute_command('BLPOP', callbacks, *tokens)

    def brpop(self, keys, timeout=0, callbacks=None):
        tokens = list(keys)
        tokens.append(timeout)
        self.execute_command('BRPOP', callbacks, *tokens)

    def lindex(self, key, index, callbacks=None):
        self.execute_command('LINDEX', callbacks, key, index)

    def llen(self, key, callbacks=None):
        self.execute_command('LLEN', callbacks, key)

    def lrange(self, key, start, end, callbacks=None):
        self.execute_command('LRANGE', callbacks, key, start, end)

    def lrem(self, key, value, num=0, callbacks=None):
        self.execute_command('LREM', callbacks, key, num, value)

    def lset(self, key, index, value, callbacks=None):
        self.execute_command('LSET', callbacks, key, index, value)

    def ltrim(self, key, start, end, callbacks=None):
        self.execute_command('LTRIM', callbacks, key, start, end)

    def lpush(self, key, value, callbacks=None):
        self.execute_command('LPUSH', callbacks, key, value)

    def rpush(self, key, value, callbacks=None):
        self.execute_command('RPUSH', callbacks, key, value)

    def lpop(self, key, callbacks=None):
        self.execute_command('LPOP', callbacks, key)

    def rpop(self, key, callbacks=None):
        self.execute_command('RPOP', callbacks, key)

    def rpoplpush(self, src, dst, callbacks=None):
        self.execute_command('RPOPLPUSH', callbacks, src, dst)

    ### SET COMMANDS
    def sadd(self, key, value, callbacks=None):
        self.execute_command('SADD', callbacks, key, value)

    def srem(self, key, value, callbacks=None):
        self.execute_command('SREM', callbacks, key, value)

    def scard(self, key, callbacks=None):
        self.execute_command('SCARD', callbacks, key)

    def spop(self, key, callbacks=None):
        self.execute_command('SPOP', callbacks, key)

    def smove(self, src, dst, value, callbacks=None):
        self.execute_command('SMOVE', callbacks, src, dst, value)

    def sismember(self, key, value, callbacks=None):
        self.execute_command('SISMEMBER', callbacks, key, value)

    def smembers(self, key, callbacks=None):
        self.execute_command('SMEMBERS', callbacks, key)

    def srandmember(self, key, callbacks=None):
        self.execute_command('SRANDMEMBER', callbacks, key)

    def sinter(self, keys, callbacks=None):
        self.execute_command('SINTER', callbacks, *keys)

    def sdiff(self, keys, callbacks=None):
        self.execute_command('SDIFF', callbacks, *keys)

    def sunion(self, keys, callbacks=None):
        self.execute_command('SUNION', callbacks, *keys)

    def sinterstore(self, keys, dst, callbacks=None):
        self.execute_command('SINTERSTORE', callbacks, dst, *keys)

    def sunionstore(self, keys, dst, callbacks=None):
        self.execute_command('SUNIONSTORE', callbacks, dst, *keys)

    def sdiffstore(self, keys, dst, callbacks=None):
        self.execute_command('SDIFFSTORE', callbacks, dst, *keys)

    ### SORTED SET COMMANDS
    def zadd(self, key, score, value, callbacks=None):
        self.execute_command('ZADD', callbacks, key, score, value)

    def zcard(self, key, callbacks=None):
        self.execute_command('ZCARD', callbacks, key)

    def zincrby(self, key, value, amount, callbacks=None):
        self.execute_command('ZINCRBY', callbacks, key, amount, value)

    def zrank(self, key, value, callbacks=None):
        self.execute_command('ZRANK', callbacks, key, value)

    def zrevrank(self, key, value, callbacks=None):
        self.execute_command('ZREVRANK', callbacks, key, value)

    def zrem(self, key, value, callbacks=None):
        self.execute_command('ZREM', callbacks, key, value)

    def zscore(self, key, value, callbacks=None):
        self.execute_command('ZSCORE', callbacks, key, value)

    def zrange(self, key, start, num, with_scores, callbacks=None):
        tokens = [key, start, num]
        if with_scores:
            tokens.append('WITHSCORES')
        self.execute_command('ZRANGE', callbacks, *tokens)

    def zrevrange(self, key, start, num, with_scores, callbacks=None):
        tokens = [key, start, num]
        if with_scores:
            tokens.append('WITHSCORES')
        self.execute_command('ZREVRANGE', callbacks, *tokens)

    def zrangebyscore(self, key, start, end, offset=None, limit=None, with_scores=False, callbacks=None):
        tokens = [key, start, end]
        if offset is not None:
            tokens.append('LIMIT')
            tokens.append(offset)
            tokens.append(limit)
        if with_scores:
            tokens.append('WITHSCORES')
        self.execute_command('ZRANGEBYSCORE', callbacks, *tokens)

    ### HASH COMMANDS
    def hgetall(self, key, callbacks=None):
        self.execute_command('HGETALL', callbacks, key)

    def hmset(self, key, mapping, callbacks=None):
        items = []
        [ items.extend(pair) for pair in mapping.iteritems() ]
        self.execute_command('HMSET', callbacks, key, *items)

    def hset(self, key, field, value, callbacks=None):
        self.execute_command('HSET', callbacks, key, field, value)

    def hget(self, key, field, callbacks=None):
        self.execute_command('HGET', callbacks, key, field)

    def hdel(self, key, field, callbacks=None):
        self.execute_command('HDEL', callbacks, key, field)

    def hlen(self, key, callbacks=None):
        self.execute_command('HLEN', callbacks, key)

    def hexists(self, key, field, callbacks=None):
        self.execute_command('HEXISTS', callbacks, key, field)

    def hincrby(self, key, field, amount=1, callbacks=None):
        self.execute_command('HINCRBY', callbacks, key, field, amount)

    def hkeys(self, key, callbacks=None):
        self.execute_command('HKEYS', callbacks, key)

    def hmget(self, key, fields, callbacks=None):
        self.execute_command('HMGET', callbacks, key, *fields)

    def hvals(self, key, callbacks=None):
        self.execute_command('HVALS', callbacks, key)

    ### PUBSUB
    def subscribe(self, channels, callbacks=None):
        callbacks = callbacks or []
        if isinstance(channels, basestring):
            channels = [channels]
        callbacks = list(callbacks) + [self.on_subscribed]
        self.execute_command('SUBSCRIBE', callbacks, *channels)

    def on_subscribed(self, result):
        (e, _) = result
        if not e:
            self.subscribed = True

    def unsubscribe(self, channels, callbacks=None):
        callbacks = callbacks or []
        if isinstance(channels, basestring):
            channels = [channels]
        callbacks = list(callbacks) + [self.on_unsubscribed]
        self.execute_command('UNSUBSCRIBE', callbacks, *channels)

    def on_unsubscribed(self, result):
        (e, _) = result
        if not e:
            self.subscribed = False

    def publish(self, channel, message, callbacks=None):
        self.execute_command('PUBLISH', callbacks, channel, message)

    def listen(self, callbacks=None):
        # 'LISTEN' is just for exception information, it is not actually sent anywhere
        callbacks = callbacks or []
        if not hasattr(callbacks, '__iter__'):
            callbacks = [callbacks]
        if self.on_message not in callbacks:
            callbacks = list(callbacks) + [self.on_message]
        self.schedule('LISTEN', callbacks)
        self.try_to_loop()

    def on_message(self, _result):
        if self.subscribed:
            self.schedule('LISTEN', self.current_task.callbacks)
            self.try_to_loop()

