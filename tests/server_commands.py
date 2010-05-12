import rutabaga
import unittest
from tornado.ioloop import IOLoop
from functools import partial


class TestIOLoop(unittest.TestCase):
    def setUp(self):
        self.loop = IOLoop()
        self.client = rutabaga.Client(io_loop=self.loop)
        self.client.connection.connect()
        self.client.flushdb()

    def tearDown(self):
        pass

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
        def on_result(step, result, error):
            if step == 1:
                self.assertEqual(result, True)
            elif step == 2:
                self.assertEqual(result, 'bar')
                self.finish()
            self.client.set('foo', 'bar', partial(on_result, 1))
            self.client.get('foo', partial(on_result, 2))
            self.start()

    def test_dbinfo(self):
        def on_result(step, result, error):
            if step == 1:
                self.assertEqual(result, True)
            elif step == 2:
                self.assertEqual(result, 2)
                self.finish()
            self.client.set('a', 1, partial(on_result, 1))
            self.client.set('b', 2, partial(on_result, 1))
            self.client.dbsize(partial(on_result, 2))
            self.start()

