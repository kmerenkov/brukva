import rutabaga
import tornado.httpserver
import tornado.web
import tornado.websocket
import tornado.ioloop
from functools import partial
import redis


r = redis.Redis(host='apodora', db=9)


async = partial(rutabaga.adisp.async, cbname='callbacks')


c = rutabaga.Client(host='apodora')
c.connect()

c.select(9)
c.set('foo', 'bar')
c.set('foo2', 'bar2')


class RutabagaHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @rutabaga.adisp.process
    def get(self):
        ((_, foo), (_, foo2)) = yield [ async(c.get)('foo'), async(c.get)('foo2') ]
        self.set_header('Content-Type', 'text/plain')
        self.write(foo)
        self.write(foo2)
        self.finish()


class RedisHandler(tornado.web.RequestHandler):
    def get(self):
        foo = r.get('foo')
        foo2 = r.get('foo2')
        self.set_header('Content-Type', 'text/plain')
        self.write(foo)
        self.write(foo2)
        self.finish()


class HelloHandler(tornado.web.RequestHandler):
    def get(self):
        self.set_header('Content-Type', 'text/plain')
        self.write('Hello world!')
        self.finish()


application = tornado.web.Application([
    (r'/rutabaga', RutabagaHandler),
    (r'/redis', RedisHandler),
    (r'/hello', HelloHandler),
])


if __name__ == '__main__':
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(8888)
    tornado.ioloop.IOLoop.instance().start()
