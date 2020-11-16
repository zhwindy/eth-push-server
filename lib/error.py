#!/usr/bin/env python
# @DESC错误和异常管理（从dsc_coin项目沿用）


class CFGError(Exception):
    pass


class UrlError(Exception):
    pass


class PayloadError(Exception):
    pass


class RequestError(Exception):
    def __init__(self, code, text=None, obj=None):
        self.code, self.text, self.obj = code, text, obj


class JsonRpcError(Exception):
    def __init__(self, code, msg):
        self.code, self.msg = code, msg


class SyncError(Exception):
    def __init__(self, height, msg):
        self.height, self.msg = height, msg


class ForkError(Exception):
    def __init__(self, height, msg):
        self.height, self.msg = height, msg
