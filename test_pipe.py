import brukva
c = brukva.Client()
c.connect()

c.set('gt', 'er')

p = c.pipeline()

p.set('foo', 'bar')
p.get('foo')
p.sadd('zar', '1')
p.sadd('zar', '4')
p.smembers('zar')
p.scard('zar')


def on_resp(res):
    from pprint import pprint
    pprint(repr(res))

from functools import partial
import time

c._io_loop.add_timeout(
    time.time()+0.1,
    partial(    p.execute, [on_resp,] )
)
c._io_loop.add_timeout(
    time.time()+2.0,
    c._io_loop.stop
)
#c._io_loop.add_callback(
#    partial(    p.execute, [on_resp,] )
#)
c.connection._stream.io_loop.start()
