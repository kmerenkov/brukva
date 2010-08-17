class RedisError(Exception):
    pass


class ConnectionError(RedisError):
    pass


class ResponseError(RedisError):
    def __init__(self, message, cmd_line):
        self.message = message
        self.cmd_line = cmd_line

    def __repr__(self):
        return 'ResponseError (on %s [%s, %s]): %s' % (self.cmd_line.cmd, self.cmd_line.args, self.cmd_line.kwargs, self.message)

    __str__ = __repr__


class InvalidResponse(RedisError):
    pass
