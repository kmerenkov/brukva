#! /usr/bin/env python

from functools import partial
import time
import os

import brukva

c = brukva.Client()
c.connect()

def delayed(dt, cmd,  *args, **kwargs):
    c._io_loop.add_timeout(
        time.time()+dt,
        partial(cmd, *args, **kwargs)
    )

def ac(cmd, *args, **kwargs):
    c._io_loop.add_callback(
        partial(cmd, *args, **kwargs)
    )

# FIXME!
#c.set('gt', 'er')

p = c.pipeline()

p.set('foo', 'bar')
p.get('foo')
p.sadd('zar', '1')
p.sadd('zar', '4')
p.smembers('zar')
p.scard('zar')

stt = time.time()
def on_resp(res):
    from pprint import pprint
    pprint(res)
    #print "%d(2)" %
    print (time.time() - stt)

ac( p.execute, [on_resp,])

delayed(0.1, p.set, 'aaa', '132')
delayed(0.1, p.set, 'bbb', 'eft')
delayed(0.1, p.mget, ('aaa', 'bbb'))
delayed(0.1, p.execute, [on_resp,])

delayed(0.2, os.sys.exit)
c.connection._stream.io_loop.start()
