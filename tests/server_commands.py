import rutabaga
import unittest
from tornado.ioloop import IOLoop
from functools import partial


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
        def on_result(result, error):
            self.assertEqual(result, True)
            self.finish()
        self.client.set('foo', 'bar', on_result)
        self.start()

    def test_get(self):
        steps = []
        def on_result(step, result, error):
            steps.append(step)
            if step == 1:
                self.assertEqual(result, True)
            elif step == 2:
                self.assertEqual(result, 'bar')
                self.finish()
        self.client.set('foo', 'bar', partial(on_result, 1))
        self.client.get('foo', partial(on_result, 2))
        self.start()
        self.assertEqual(steps, [1, 2])

    def test_dbinfo(self):
        steps = []
        def on_result(step, result, error):
            steps.append(step)
            if step == 1:
                self.assertEqual(result, True)
            elif step == 2:
                self.assertEqual(result, 2)
                self.finish()
            self.client.set('a', 1, partial(on_result, 1))
            self.client.set('b', 2, partial(on_result, 1))
            self.client.dbsize(partial(on_result, 2))
            self.start()
            self.assertEqual(steps, [1, 2])

    def test_hash(self):
        steps = []
        def on_result(step, result, error):
            steps.append(step)
            if error:
                raise error
            if step == 1:
                self.assertEqual(result, True)
            elif step == 2:
                self.assertEqual(result, {'a': '1', 'b': '2'})
            elif step == 3:
                self.assertEqual(result, True)
            elif step == 4:
                self.assertEqual(result, {'b': '2'})
            elif step == 5:
                self.assertEqual(result, '')
            elif step == 6:
                self.assertEqual(result, '2')
            elif step == 7:
                self.assertEqual(result, 1)
                self.finish()
        self.client.hmset('foo', {'a': 1, 'b': 2}, partial(on_result, 1))
        self.client.hgetall('foo', partial(on_result, 2))
        self.client.hdel('foo', 'a', partial(on_result, 3))
        self.client.hgetall('foo', partial(on_result, 4))
        self.client.hget('foo', 'a', partial(on_result, 5))
        self.client.hget('foo', 'b', partial(on_result, 6))
        self.client.hlen('foo', partial(on_result, 7))
        self.start()
        self.assertEqual(steps, [1, 2, 3, 4, 5, 6, 7])

    def test_incrdecr(self):
        steps = []
        def on_result(step, result, error):
            steps.append(step)
            if error:
                raise error
            if step == 1:
                self.assertEqual(result, 1)
            elif step == 2:
                self.assertEqual(result, 11)
            elif step == 3:
                self.assertEqual(result, 10)
            elif step == 4:
                self.assertEqual(result, 0)
            elif step == 5:
                self.assertEqual(result, -1)
                self.finish()
        self.client.incr('foo', partial(on_result, 1))
        self.client.incrby('foo', 10, partial(on_result, 2))
        self.client.decr('foo', partial(on_result, 3))
        self.client.decrby('foo', 10, partial(on_result, 4))
        self.client.decr('foo', partial(on_result, 5))
        self.start()
        self.assertEqual(steps, [1, 2, 3, 4, 5])

    def test_ping(self):
        steps = []
        def on_result(result, error):
            steps.append(1)
            self.assertEqual(result, True)
            self.finish()
        self.client.ping(on_result)
        self.start()
        self.assertEqual(steps, [1])

    def test_lists(self):
        steps = []
        def on_result(step, result, error):
            steps.append(step)
            if step == 1:
                self.assertEqual(result, True)
            elif step == 2:
                self.assertEqual(result, 1)
            elif step == 3:
                self.assertEqual(result, ['1'])
            elif step == 4:
                self.assertEqual(result, '1')
            elif step == 5:
                self.assertEqual(result, 0)
                self.finish()
        self.client.lpush('foo', 1, partial(on_result, 1))
        self.client.llen('foo', partial(on_result, 2))
        self.client.lrange('foo', 0, -1, partial(on_result, 3))
        self.client.rpop('foo', partial(on_result, 4))
        self.client.llen('foo', partial(on_result, 5))
        self.start()
        self.assertEqual(steps, [1, 2, 3, 4, 5])
