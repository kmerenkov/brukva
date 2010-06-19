import brukva
import unittest
import sys
from datetime import datetime, timedelta
from tornado.ioloop import IOLoop


class CustomAssertionError(AssertionError):
    io_loop = None

    def __init__(self, *args, **kwargs):
        super(CustomAssertionError, self).__init__(*args, **kwargs)
        CustomAssertionError.io_loop.stop()


class TestIOLoop(IOLoop):
    def handle_callback_exception(self, callback):
        (type, value, traceback) = sys.exc_info()
        raise type, value, traceback


class TornadoTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TornadoTestCase, self).__init__(*args, **kwargs)
        self.failureException = CustomAssertionError

    def setUp(self):
        self.loop = TestIOLoop()
        CustomAssertionError.io_loop = self.loop
        self.client = brukva.Client(io_loop=self.loop)
        self.client.connection.connect()
        self.client.select(9)
        self.client.flushdb()

    def tearDown(self):
        self.finish()

    def expect(self, expected):
        def callback(result):
            (error, data) = result
            if error:
                self.assertFalse(error)
            if callable(expected):
                self.assertTrue(expected(data))
            else:
                self.assertEqual(expected, data)
        return callback

    def finish(self, *args):
        self.loop.stop()

    def start(self):
        self.loop.start()


class ServerCommandsTestCase(TornadoTestCase):
    def test_set(self):
        self.client.set('foo', 'bar', [self.expect(True), self.finish])
        self.start()

    def test_setex(self):
        self.client.setex('foo', 5, 'bar', self.expect(True))
        self.client.ttl('foo', [self.expect(5), self.finish])
        self.start()

    def test_setnx(self):
        self.client.setnx('a', 1, self.expect(True))
        self.client.setnx('a', 0, [self.expect(False), self.finish])
        self.start()

    def test_get(self):
        self.client.set('foo', 'bar', self.expect(True))
        self.client.get('foo', [self.expect('bar'), self.finish])
        self.start()

    def test_randomkey(self):
        self.client.set('a', 1, self.expect(True))
        self.client.set('b', 1, self.expect(True))
        self.client.randomkey(self.expect(lambda k: k in ['a', 'b']))
        self.client.randomkey(self.expect(lambda k: k in ['a', 'b']))
        self.client.randomkey([self.expect(lambda k: k in ['a', 'b']), self.finish])
        self.start()

    def test_substr(self):
        self.client.set('foo', 'lorem ipsum', self.expect(True))
        self.client.substr('foo', 2, 4, [self.expect('rem'), self.finish])
        self.start()

    def test_append(self):
        self.client.set('foo', 'lorem ipsum', self.expect(True))
        self.client.append('foo', ' bar', self.expect(15))
        self.client.get('foo', [self.expect('lorem ipsum bar'), self.finish])
        self.start()

    def test_dbsize(self):
        self.client.set('a', 1, self.expect(True))
        self.client.set('b', 2, self.expect(True))
        self.client.dbsize([self.expect(2), self.finish])
        self.start()

    def test_save(self):
        self.client.save(self.expect(True))
        now = datetime.now().replace(microsecond=0)
        self.client.lastsave([self.expect(lambda d: d >= now), self.finish])
        self.start()

    def test_keys(self):
        self.client.set('a', 1, self.expect(True))
        self.client.set('b', 2, self.expect(True))
        self.client.keys('*', self.expect(['a', 'b']))
        self.client.keys('', self.expect([]))

        self.client.set('foo_a', 1, self.expect(True))
        self.client.set('foo_b', 2, self.expect(True))
        self.client.keys('foo_*', [self.expect(['foo_a', 'foo_b']), self.finish])
        self.start()

    def test_expire(self):
        self.client.set('a', 1, self.expect(True))
        self.client.expire('a', 10, self.expect(True))
        self.client.ttl('a', [self.expect(10), self.finish])
        self.start()

    def test_type(self):
        self.client.set('a', 1, self.expect(True))
        self.client.type('a', self.expect('string'))
        self.client.rpush('b', 1, self.expect(True))
        self.client.type('b', self.expect('list'))
        self.client.sadd('c', 1, self.expect(True))
        self.client.type('c', self.expect('set'))
        self.client.hset('d', 'a', 1, self.expect(True))
        self.client.type('d', self.expect('hash'))
        self.client.zadd('e', 1, 1, self.expect(True))
        self.client.type('e', [self.expect('zset'), self.finish])
        self.start()

    def test_rename(self):
        self.client.set('a', 1, self.expect(True))
        self.client.rename('a', 'b', self.expect(True))
        self.client.set('c', 1, self.expect(True))
        self.client.renamenx('c', 'b', [self.expect(False), self.finish])
        self.start()

    def test_move(self):
        self.client.select(8, self.expect(True))
        self.client.delete('a', self.expect(True))
        self.client.select(9, self.expect(True))
        self.client.set('a', 1, self.expect(True))
        self.client.move('a', 8, self.expect(True))
        self.client.exists('a', self.expect(False))
        self.client.select(8, self.expect(True))
        self.client.get('a', [self.expect('1'), self.finish])
        self.start()

    def test_exists(self):
        self.client.set('a', 1, self.expect(True))
        self.client.exists('a', self.expect(True))
        self.client.delete('a', self.expect(True))
        self.client.exists('a', [self.expect(False), self.finish])
        self.start()

    def test_mset_mget(self):
        self.client.mset({'a': 1, 'b': 2}, self.expect(True))
        self.client.get('a', self.expect('1'))
        self.client.get('b', self.expect('2'))
        self.client.mget(['a', 'b'], [self.expect(['1', '2']), self.finish])
        self.start()

    def test_msetnx(self):
        self.client.msetnx({'a': 1, 'b': 2}, self.expect(True))
        self.client.msetnx({'b': 3, 'c': 4}, [self.expect(False), self.finish])
        self.start()

    def test_getset(self):
        self.client.set('a', 1, self.expect(True))
        self.client.getset('a', 2, self.expect('1'))
        self.client.get('a', [self.expect('2'), self.finish])
        self.start()

    def test_hash(self):
        self.client.hmset('foo', {'a': 1, 'b': 2}, self.expect(True))
        self.client.hgetall('foo', self.expect({'a': '1', 'b': '2'}))
        self.client.hdel('foo', 'a', self.expect(True))
        self.client.hgetall('foo', self.expect({'b': '2'}))
        self.client.hget('foo', 'a', self.expect(''))
        self.client.hget('foo', 'b', self.expect('2'))
        self.client.hlen('foo', self.expect(1))
        self.client.hincrby('foo', 'b', 3, self.expect(5))
        self.client.hkeys('foo', self.expect(['b']))
        self.client.hvals('foo', self.expect(['5']))
        self.client.hmget('foo', 'b', self.expect(['5']))
        self.client.hexists('foo', 'b', [self.expect(True), self.finish])
        self.start()

    def test_incrdecr(self):
        self.client.incr('foo', self.expect(1))
        self.client.incrby('foo', 10, self.expect(11))
        self.client.decr('foo', self.expect(10))
        self.client.decrby('foo', 10, self.expect(0))
        self.client.decr('foo', [self.expect(-1), self.finish])
        self.start()

    def test_ping(self):
        self.client.ping([self.expect(True), self.finish])
        self.start()

    def test_lists(self):
        self.client.lpush('foo', 1, self.expect(True))
        self.client.llen('foo', self.expect(1))
        self.client.lrange('foo', 0, -1, self.expect(['1']))
        self.client.rpop('foo', self.expect('1'))
        self.client.llen('foo', [self.expect(0), self.finish])
        self.start()

    def test_sets(self):
        self.client.smembers('foo', self.expect(set()))
        self.client.sadd('foo', 'a', self.expect(1))
        self.client.sadd('foo', 'b', self.expect(1))
        self.client.sadd('foo', 'c', self.expect(1))
        self.client.srandmember('foo', self.expect(lambda x: x in ['a', 'b', 'c']))
        self.client.scard('foo', self.expect(3))
        self.client.srem('foo', 'a', self.expect(True))
        self.client.smove('foo', 'bar', 'b', self.expect(True))
        self.client.smembers('bar', self.expect(set(['b'])))
        self.client.sismember('foo', 'c', self.expect(True))
        self.client.spop('foo', [self.expect('c'), self.finish])
        self.start()

    def test_sets2(self):
        self.client.sadd('foo', 'a', self.expect(1))
        self.client.sadd('foo', 'b', self.expect(1))
        self.client.sadd('foo', 'c', self.expect(1))
        self.client.sadd('bar', 'b', self.expect(1))
        self.client.sadd('bar', 'c', self.expect(1))
        self.client.sadd('bar', 'd', self.expect(1))

        self.client.sdiff(['foo', 'bar'], self.expect(set(['a'])))
        self.client.sdiff(['bar', 'foo'], self.expect(set(['d'])))
        self.client.sinter(['foo', 'bar'], self.expect(set(['b', 'c'])))
        self.client.sunion(['foo', 'bar'], [self.expect(set(['a', 'b', 'c', 'd'])), self.finish])
        self.start()

    def test_sets3(self):
        self.client.sadd('foo', 'a', self.expect(1))
        self.client.sadd('foo', 'b', self.expect(1))
        self.client.sadd('foo', 'c', self.expect(1))
        self.client.sadd('bar', 'b', self.expect(1))
        self.client.sadd('bar', 'c', self.expect(1))
        self.client.sadd('bar', 'd', self.expect(1))

        self.client.sdiffstore(['foo', 'bar'], 'zar', self.expect(1))
        self.client.smembers('zar', self.expect(set(['a'])))
        self.client.delete('zar', self.expect(True))

        self.client.sinterstore(['foo', 'bar'], 'zar', self.expect(2))
        self.client.smembers('zar', self.expect(set(['b', 'c'])))
        self.client.delete('zar', self.expect(True))

        self.client.sunionstore(['foo', 'bar'], 'zar', self.expect(4))
        self.client.smembers('zar', [self.expect(set(['a', 'b', 'c', 'd'])), self.finish])
        self.start()

    def test_zsets(self):
        self.client.zadd('foo', 1, 'a', self.expect(1))
        self.client.zadd('foo', 2, 'b', self.expect(1))
        self.client.zscore('foo', 'a', self.expect(1))
        self.client.zscore('foo', 'b', self.expect(2))
        self.client.zrank('foo', 'a', self.expect(0))
        self.client.zrank('foo', 'b', self.expect(1))
        self.client.zrevrank('foo', 'a', self.expect(1))
        self.client.zrevrank('foo', 'b', self.expect(0))
        self.client.zincrby('foo', 'a', 1, self.expect(2))
        self.client.zincrby('foo', 'b', 1, self.expect(3))
        self.client.zscore('foo', 'a', self.expect(2))
        self.client.zscore('foo', 'b', self.expect(3))
        self.client.zrange('foo', 0, -1, True, self.expect([('a', 2.0), ('b', 3.0)]))
        self.client.zrange('foo', 0, -1, False, self.expect(['a', 'b']))
        self.client.zrevrange('foo', 0, -1, True, self.expect([('b', 3.0), ('a', 2.0)]))
        self.client.zrevrange('foo', 0, -1, False, self.expect(['b', 'a']))
        self.client.zcard('foo', [self.expect(2)])
        self.client.zadd('foo', 3.5, 'c', self.expect(1))
        self.client.zrangebyscore('foo', '-inf', '+inf', None, None, False, self.expect(['a', 'b', 'c']))
        self.client.zrangebyscore('foo', '2.1', '+inf', None, None, True, self.expect([('b', 3.0), ('c', 3.5)]))
        self.client.zrangebyscore('foo', '-inf', '3.0', 0, 1, False, self.expect(['a']))
        self.client.zrangebyscore('foo', '-inf', '+inf', 1, 2, False, self.expect(['b', 'c']))

        self.client.delete('foo', self.expect(True))
        self.client.zadd('foo', 1, 'a', self.expect(1))
        self.client.zadd('foo', 2, 'b', self.expect(1))
        self.client.zadd('foo', 3, 'c', self.expect(1))
        self.client.zadd('foo', 4, 'd', self.expect(1))
        self.client.zremrangebyrank('foo', 2, 4, self.expect(2))
        self.client.zremrangebyscore('foo', 0, 2, [self.expect(2), self.finish()])

        self.client.zadd('a', 1, 'a1', self.expect(1))
        self.client.zadd('a', 1, 'a2', self.expect(1))
        self.client.zadd('a', 1, 'a3', self.expect(1))
        self.client.zadd('b', 2, 'a1', self.expect(1))
        self.client.zadd('b', 2, 'a3', self.expect(1))
        self.client.zadd('b', 2, 'a4', self.expect(1))
        self.client.zadd('c', 6, 'a1', self.expect(1))
        self.client.zadd('c', 5, 'a3', self.expect(1))
        self.client.zadd('c', 4, 'a4', self.expect(1))

        # ZINTERSTORE
        # sum, no weight
        self.client.zinterstore('z', ['a', 'b', 'c'], callbacks=self.expect(2))
        self.client.zrange('z', 0, -1, with_scores=True, callbacks=self.expect([('a3', 8),
                                                                                ('a1', 9),
                                                                                ]))
        # max, no weight
        self.client.zinterstore('z', ['a', 'b', 'c'], aggregate='MAX', callbacks=self.expect(2))
        self.client.zrange('z', 0, -1, with_scores=True, callbacks=self.expect([('a3', 5),
                                                                                ('a1', 6),
                                                                                ]))
        # with weight
        self.client.zinterstore('z', {'a': 1, 'b': 2, 'c': 3}, callbacks=self.expect(2))
        self.client.zrange('z', 0, -1, with_scores=True, callbacks=[self.expect([('a3', 20),
                                                                                 ('a1', 23),
                                                                                 ]),
                                                                    self.finish()])

        # ZUNIONSTORE
        # sum, no weight
        self.client.zunionstore('z', ['a', 'b', 'c'], callbacks=self.expect(5))
        self.client.zrange('z', 0, -1, with_scores=True, callbacks=self.expect([('a2', 1),
                                                                                ('a3', 3),
                                                                                ('a5', 4),
                                                                                ('a4', 7),
                                                                                ('a1', 9),
                                                                                ]))
        # max, no weight
        self.client.zunionstore('z', ['a', 'b', 'c'], aggregate='MAX', callbacks=self.expect(5))
        self.client.zrange('z', 0, -1, with_scores=True, callbacks=self.expect([('a2', 1),
                                                                                ('a3', 2),
                                                                                ('a5', 4),
                                                                                ('a4', 5),
                                                                                ('a1', 6),
                                                                                ]))
        # with weight
        self.client.zunionstore('z', {'a': 1, 'b': 2, 'c': 3}, callbacks=self.expect(5))
        self.client.zrange('z', 0, -1, with_scores=True, callbacks=[self.expect([('a2', 1),
                                                                                 ('a3', 5),
                                                                                 ('a5', 12),
                                                                                 ('a4', 19),
                                                                                 ('a1', 23),
                                                                                 ]),
                                                                    self.finish(),
                                                                    ])
        self.start()


if __name__ == '__main__':
    unittest.main()
