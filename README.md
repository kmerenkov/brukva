rutabaga
========

Asynchronous [Redis](http://redis-db.com/) client that works within [Tornado](http://tornadoweb.org/) IO loop.


Usage
-----

    >>> import rutabaga
    >>> c = rutabaga.Client()
    >>> c.connect()
    >>> def on_result(result):
           (error, data) = result
           print data or error
    >>> c.set('foo', 'bar', on_result)
    >>> c.get('foo', on_result)
    >>> c.hgetall('foo', on_result)
    >>> c.connection._stream.io_loop.start() # start tornado mainloop
    True
    bar
    ResponseError (on HGETALL [('foo',), {}]): Operation against a key holding the wrong kind of value


Credits
-------
rutabaga is developed and maintained by [Konstantin Merenkov](mailto:kmerenkov@gmail.com)

 * Inspiration: [redis-py](http://github.com/andymccurdy/redis-py)
 * Third-party software: [adisp](https://code.launchpad.net/adisp)

