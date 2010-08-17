#! /usr/bin/env python

from functools import partial
import time
import os
from pprint import pprint

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

stt = time.time()
def on_resp(res):

    pprint(res)
    print (time.time() - stt)

c.set('gt', 'er', on_resp)
c.get('gt', on_resp)

p = c.pipeline()

p.set('foo', 'bar')
p.get('foo')
p.set('bar', '123')
p.mget(['foo', 'bar',])
p.sadd('zar', '1')
p.sadd('zar', '4')
p.smembers('zar')
p.scard('zar')

ac( p.execute, [on_resp,])

delayed(0.1, p.set, 'aaa', '132')
delayed(0.1, p.set, 'bbb', 'eft')
delayed(0.1, p.mget, ('aaa', 'bbb'))
delayed(0.1, p.execute, [on_resp,])

delayed(0.3, os.sys.exit)
c.connection._stream.io_loop.start()
