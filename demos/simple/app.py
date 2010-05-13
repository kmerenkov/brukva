import rutabaga
import tornado.httpserver
import tornado.web
import tornado.websocket
import tornado.ioloop
from rutabaga import adisp
import logging
from functools import partial


logging.basicConfig(level=logging.DEBUG)

log = logging.getLogger('app')


c = rutabaga.Client()
c.connect()


def on_set(result):
    (error, data) = result
    log.debug("set result: %s" % (error or data,))


async = partial(adisp.async, cbname='callbacks')


c.set('foo', 'Lorem ipsum #1', on_set)
c.set('bar', 'Lorem ipsum #2', on_set)
c.set('zar', 'Lorem ipsum #3', on_set)


class MainHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @adisp.process
    def get(self):
        (_, foo) = yield async(c.get)('foo')
        (_, bar) = yield async(c.get)('bar')
        (_, zar) = yield async(c.get)('zar')
        self.set_header('Content-Type', 'text/html')
        self.render("template.html", title="Simple demo", foo=foo, bar=bar, zar=zar)


application = tornado.web.Application([
    (r'/', MainHandler),
])


if __name__ == '__main__':
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(8888)
    tornado.ioloop.IOLoop.instance().start()
