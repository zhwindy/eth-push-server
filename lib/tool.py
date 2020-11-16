#!/usr/bin/env python

import rlp
import time
import datetime
import traceback
from core.globals import G_LOGGER, G_CFG
from sha3 import keccak_256 as _sha3


def int_to_hex(_int):
    """
    int 10进制转16进制字符
    :param _int:
    :return:
    """
    return hex(_int)


def hex_to_int(_hex):
    """
    16进制转10进制 int
    :param _hex:
    :return:
    """
    return int(_hex, base=16)


def hex_to_bytes(_hex):
    """
    16进制字符串转bytes类型
    :param _hex:
    :return:
    """
    return bytes.fromhex(_hex)


def date_to_timestamp(t):
    """
    日期字符串转时间戳
    :param t:
    :return:
    """
    d = datetime.datetime.strptime(t, '%Y-%m-%dT%H:%M:%S')
    delta = datetime.timedelta(hours=8)
    timestamp = int(time.mktime((d + delta).timetuple()))
    return timestamp


def date_z_to_timestamp(t):
    """
    日期字符串转时间戳
    :param t:
    :return:
    """
    d = datetime.datetime.strptime(t, '%Y-%m-%dT%H:%M:%SZ')
    delta = datetime.timedelta(hours=-8)
    timestamp = int(time.mktime((d + delta).timetuple()))
    return timestamp


def utcstr_to_timestamp(utcstr):
    dt = datetime.datetime.strptime(utcstr[:19], "%Y-%m-%dT%H:%M:%S")
    return int((dt + datetime.timedelta(hours=8)).timestamp())


def check_0x_type(_hex):
    """
    检测类型
    :param _hex:
    :return:
    """
    if isinstance(_hex, (str, bytes)):
        _type = bytes if isinstance(_hex, bytes) else str
        return _type
    raise ValueError('_hex must be str or bytes')


def del_0x(_hex):
    """
    去掉0x
    :param _hex:
    :return:
    """
    _type = check_0x_type(_hex)
    _0x = '0x' if _type is str else b'0x'
    if _hex.lower().startswith(_0x):
        return _hex.lstrip(_0x)
    return _hex


def add_0x(_hex):
    """
    增加0x
    :param _hex:
    :return:
    """
    _type = check_0x_type(_hex)
    _0x = '0x' if _type is str else b'0x'
    if not _hex.lower().startswith(_0x):
        return _0x + _hex
    return _hex


def size_convert(_hex):
    """
    大小端转换
    :param _hex:
    :return:
    """
    assert isinstance(_hex, (str,))
    return bytes.fromhex(_hex)[::-1].hex()


def get_now(local=False):
    """获取当前时间戳"""
    if local:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    return int(time.time())


def full_url(url, path):
    if not url:
        raise Exception("url is null, please check rpc url config")
    if url[-1] == "/":
        url = url[:-1]
    if path[0] == "/":
        path = path[1:]
    return f"{url}/{path}"


def sha(*args):
    s = _sha3()
    for arg in args:
        s.update(arg)
    return s.hexdigest()


def sha3(r):
    return _sha3(r).digest()


def make_address(addr, nonce):
    if not isinstance(addr, (bytes, str)):
        raise TypeError

    if isinstance(addr, str):
        addr = bytes.fromhex(addr)

    if len(addr) != 20:
        raise ValueError("length must be 20 bytes")

    enc_list = rlp.encode([addr, nonce])
    return sha3(enc_list)[12:].hex()


def loop_wrap(func):
    """loop包装器"""
    def _wrap(*args, **kwargs):
        def _loop():
            func(*args, **kwargs)
        mode = G_CFG.coin.coin_dict["mode"]
        while True:
            if mode == "prod":
                try:
                    _loop()
                except Exception:
                    error_info = traceback.format_exc()
                    G_LOGGER.error(f"出现异常, 请处理. {error_info}")
                except KeyboardInterrupt:
                    G_LOGGER.info(f"{func.__name__}手动取消任务, 退出循环执行任务")
            elif mode == "dev":
                _loop()
            # 每秒循环一次
            time.sleep(5)
    return _wrap


def seperate_big_list(big_list, chunk=50):
    """
    把大的列表按照指定只存切分为小列表
    """
    for i in range(0, len(big_list), chunk):
        yield big_list[i: i + chunk]


class Single:
    _s = None

    def __new__(cls, *args, **kwargs):
        if cls._s is not None:
            return cls._s
        cls._s = super().__new__(cls)
        return cls._s


class CachedProperty:

    def __init__(self, f):
        self.f = f

    def __get__(self, obj, type):
        obj = obj or type
        value = self.f(obj)
        setattr(obj, self.f.__name__, value)
        return value
