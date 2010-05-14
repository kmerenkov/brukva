import rutabaga
import unittest
from tornado.ioloop import IOLoop
from functools import partial as p


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
        self.client = rutabaga.Client(io_loop=self.loop)
        self.client.connection.connect()
        self.client.select(9)
        self.client.flushdb()

    def tearDown(self):
        self.finish()

    def finish(self):
        self.loop.stop()

    def start(self):
        self.loop.start()


class ServerCommandsTestCase(TestIOLoop):
    def test_set(self):
        def on_result(result):
            (_, data) = result
            self.assertEqual(data, True)
            self.finish()
        self.client.set('foo', 'bar', on_result)
        self.start()

    def test_get(self):
        steps = []
        def on_result(step, result):
            (_, data) = result
            steps.append(step)
            if step == 1:
                self.assertEqual(data, True)
            elif step == 2:
                self.assertEqual(data, 'bar')
                self.finish()
        self.client.set('foo', 'bar', p(on_result, 1))
        self.client.get('foo', p(on_result, 2))
        self.start()
        self.assertEqual(steps, [1, 2])

    def test_dbinfo(self):
        steps = []
        def on_result(step, result):
            (_, data) = result
            steps.append(step)
            if step == 1:
                self.assertEqual(data, True)
            elif step == 2:
                self.assertEqual(data, 2)
                self.finish()
            self.client.set('a', 1, p(on_result, 1))
            self.client.set('b', 2, p(on_result, 1))
            self.client.dbsize(p(on_result, 2))
            self.start()
            self.assertEqual(steps, [1, 2])

    def test_hash(self):
        steps = []
        def on_result(step, result):
            (error, data) = result
            steps.append(step)
            if error:
                raise error
            if step == 1:
                self.assertEqual(data, True)
            elif step == 2:
                self.assertEqual(data, {'a': '1', 'b': '2'})
            elif step == 3:
                self.assertEqual(data, True)
            elif step == 4:
                self.assertEqual(data, {'b': '2'})
            elif step == 5:
                self.assertEqual(data, '')
            elif step == 6:
                self.assertEqual(data, '2')
            elif step == 7:
                self.assertEqual(data, 1)
                self.finish()
        self.client.hmset('foo', {'a': 1, 'b': 2}, p(on_result, 1))
        self.client.hgetall('foo', p(on_result, 2))
        self.client.hdel('foo', 'a', p(on_result, 3))
        self.client.hgetall('foo', p(on_result, 4))
        self.client.hget('foo', 'a', p(on_result, 5))
        self.client.hget('foo', 'b', p(on_result, 6))
        self.client.hlen('foo', p(on_result, 7))
        self.start()
        self.assertEqual(steps, [1, 2, 3, 4, 5, 6, 7])

    def test_incrdecr(self):
        steps = []
        def on_result(step, result):
            (error, data) = result
            steps.append(step)
            if error:
                raise error
            if step == 1:
                self.assertEqual(data, 1)
            elif step == 2:
                self.assertEqual(data, 11)
            elif step == 3:
                self.assertEqual(data, 10)
            elif step == 4:
                self.assertEqual(data, 0)
            elif step == 5:
                self.assertEqual(data, -1)
                self.finish()
        self.client.incr('foo', p(on_result, 1))
        self.client.incrby('foo', 10, p(on_result, 2))
        self.client.decr('foo', p(on_result, 3))
        self.client.decrby('foo', 10, p(on_result, 4))
        self.client.decr('foo', p(on_result, 5))
        self.start()
        self.assertEqual(steps, [1, 2, 3, 4, 5])

    def test_ping(self):
        steps = []
        def on_result(result):
            (_, data) = result
            steps.append(1)
            self.assertEqual(data, True)
            self.finish()
        self.client.ping(on_result)
        self.start()
        self.assertEqual(steps, [1])

    def test_lists(self):
        steps = []
        def on_result(step, result):
            (_, data) = result
            steps.append(step)
            if step == 1:
                self.assertEqual(data, True)
            elif step == 2:
                self.assertEqual(data, 1)
            elif step == 3:
                self.assertEqual(data, ['1'])
            elif step == 4:
                self.assertEqual(data, '1')
            elif step == 5:
                self.assertEqual(data, 0)
                self.finish()
        self.client.lpush('foo', 1, p(on_result, 1))
        self.client.llen('foo', p(on_result, 2))
        self.client.lrange('foo', 0, -1, p(on_result, 3))
        self.client.rpop('foo', p(on_result, 4))
        self.client.llen('foo', p(on_result, 5))
        self.start()
        self.assertEqual(steps, [1, 2, 3, 4, 5])

    def test_sets(self):
        steps = []
        def on_result(step, result):
            (_, data) = result
            steps.append(step)
            if step == 1:
                self.assertEqual(data, True)
            elif step == 1.5:
                self.assertTrue(data in ['a', 'b', 'c'])
            elif step == 2:
                self.assertEqual(data, 3)
            elif step == 3:
                self.assertEqual(data, True)
            elif step == 4:
                self.assertEqual(data, True)
            elif step == 5:
                self.assertEqual(data, set(['b']))
            elif step == 6:
                self.assertEqual(data, True)
            elif step == 7:
                self.assertEqual(data, 'c')
                self.finish()
        self.client.sadd('foo', 'a', p(on_result, 1))
        self.client.sadd('foo', 'b', p(on_result, 1))
        self.client.sadd('foo', 'c', p(on_result, 1))
        self.client.srandmember('foo', p(on_result, 1.5))
        self.client.scard('foo', p(on_result, 2))
        self.client.srem('foo', 'a', p(on_result, 3))
        self.client.smove('foo', 'bar', 'b', p(on_result, 4))
        self.client.smembers('bar', p(on_result, 5))
        self.client.sismember('foo', 'c', p(on_result, 6))
        self.client.spop('foo', p(on_result, 7))
        self.start()
        self.assertEqual(steps, [1, 1, 1, 1.5, 2, 3, 4, 5, 6, 7])

    def test_sets2(self):
        def end(_):
            self.finish()
        def expect(expected, result):
            (_, actual) = result
            self.assertEqual(expected, actual)
        self.client.sadd('foo', 'a', p(expect, True))
        self.client.sadd('foo', 'b', p(expect, True))
        self.client.sadd('foo', 'c', p(expect, True))
        self.client.sadd('bar', 'b', p(expect, True))
        self.client.sadd('bar', 'c', p(expect, True))
        self.client.sadd('bar', 'd', p(expect, True))

        self.client.sdiff(['foo', 'bar'], p(expect, set(['a'])))
        self.client.sdiff(['bar', 'foo'], p(expect, set(['d'])))
        self.client.sinter(['foo', 'bar'], p(expect, set(['b', 'c'])))
        self.client.sunion(['foo', 'bar'], [p(expect, set(['a', 'b', 'c', 'd'])), end])
        self.start()


