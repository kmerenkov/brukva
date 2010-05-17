class RedisError(Exception):
    pass


class ConnectionError(RedisError):
    pass


class ResponseError(RedisError):
    def __init__(self, task, message):
        self.task = task
        self.message = message

    def __repr__(self):
        return 'ResponseError (on %s [%s, %s]): %s' % (self.task.command, self.task.args, self.task.kwargs, self.message)

    __str__ = __repr__


class InvalidResponse(RedisError):
    pass
