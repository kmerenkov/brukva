import brukva
import unittest
from tornado.ioloop import IOLoop


class CustomAssertionError(AssertionError):
    io_loop = None

    def __init__(self, msg):
        super(CustomAssertionError, self).__init__(msg)
        CustomAssertionError.io_loop.stop()


class TestIOLoop(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestIOLoop, self).__init__(*args, **kwargs)
        self.failureException = CustomAssertionError

    def setUp(self):
        self.loop = IOLoop()
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


class ServerCommandsTestCase(TestIOLoop):
    def test_set(self):
        self.client.set('foo', 'bar', [self.expect(True), self.finish])
        self.start()

    def test_get(self):
        self.client.set('foo', 'bar', self.expect(True))
        self.client.get('foo', [self.expect('bar'), self.finish])
        self.start()

    def test_dbinfo(self):
        self.client.set('a', 1, self.expect(True))
        self.client.set('b', 2, self.expect(True))
        self.client.dbsize([self.expect(2), self.finish])
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

    def test_hash(self):
        self.client.hmset('foo', {'a': 1, 'b': 2}, self.expect(True))
        self.client.hgetall('foo', self.expect({'a': '1', 'b': '2'}))
        self.client.hdel('foo', 'a', self.expect(True))
        self.client.hgetall('foo', self.expect({'b': '2'}))
        self.client.hget('foo', 'a', self.expect(''))
        self.client.hget('foo', 'b', self.expect('2'))
        self.client.hlen('foo', self.expect(1))
        self.client.hincrby('foo', 'b', 3, self.expect('5'))
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
        self.client.sadd('foo', 'a', self.expect(True))
        self.client.sadd('foo', 'b', self.expect(True))
        self.client.sadd('foo', 'c', self.expect(True))
        self.client.srandmember('foo', self.expect(lambda x: x in ['a', 'b', 'c']))
        self.client.scard('foo', self.expect(3))
        self.client.srem('foo', 'a', self.expect(True))
        self.client.smove('foo', 'bar', 'b', self.expect(True))
        self.client.smembers('bar', self.expect(set(['b'])))
        self.client.sismember('foo', 'c', self.expect(True))
        self.client.spop('foo', [self.expect('c'), self.finish])
        self.start()

    def test_sets2(self):
        self.client.sadd('foo', 'a', self.expect(True))
        self.client.sadd('foo', 'b', self.expect(True))
        self.client.sadd('foo', 'c', self.expect(True))
        self.client.sadd('bar', 'b', self.expect(True))
        self.client.sadd('bar', 'c', self.expect(True))
        self.client.sadd('bar', 'd', self.expect(True))

        self.client.sdiff(['foo', 'bar'], self.expect(set(['a'])))
        self.client.sdiff(['bar', 'foo'], self.expect(set(['d'])))
        self.client.sinter(['foo', 'bar'], self.expect(set(['b', 'c'])))
        self.client.sunion(['foo', 'bar'], [self.expect(set(['a', 'b', 'c', 'd'])), self.finish])
        self.start()

    def test_sets3(self):
        self.client.sadd('foo', 'a', self.expect(True))
        self.client.sadd('foo', 'b', self.expect(True))
        self.client.sadd('foo', 'c', self.expect(True))
        self.client.sadd('bar', 'b', self.expect(True))
        self.client.sadd('bar', 'c', self.expect(True))
        self.client.sadd('bar', 'd', self.expect(True))

        self.client.sdiffstore(['foo', 'bar'], 'zar', self.expect(1))
        self.client.smembers('zar', self.expect(set(['a'])))
        self.client.delete('zar', self.expect(True))

        self.client.sinterstore(['foo', 'bar'], 'zar', self.expect(2))
        self.client.smembers('zar', self.expect(set(['b', 'c'])))
        self.client.delete('zar', self.expect(True))

        self.client.sunionstore(['foo', 'bar'], 'zar', self.expect(4))
        self.client.smembers('zar', [self.expect(set(['a', 'b', 'c', 'd'])), self.finish])
        self.start()

if __name__ == '__main__':
    unittest.main()
