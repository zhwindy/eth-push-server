#!/usr/bin/env python
import os
import configparser
import re
from lib.error import CFGError

BASE_PATH = os.path.abspath(os.path.dirname(__file__))


class CFG:
    __cfg = None
    # 默认配置, 如果有缺少项则按这个来
    __INTEGRANT = {
        "coin": dict(name="", process="", mode="dev"),
        "rpc": dict(url="", timeout=60),
        "mysql": dict(host="127.0.0.1", port=3306, user="root", password="", db="push_server"),
        "redis": dict(host="127.0.0.1", port=6379, password="", max_connections=100),
        "message": dict(time_interval=300, monitor_path="log/monitor.txt", mail_user="", mail_pass="", mail_host="", mail_port="", receiver=""),
        "mq": dict(host="127.0.0.1", vhost="app", port=5672, username="", password="", exchange_name="app.user.push.msg", routing_key="", step=10, start_block=0),
        "log": dict(level="INFO", filename="log/info.log", output="cmd", format="%(asctime)s-%(filename)s[line:%(lineno)d]-%(levelname)s: %(message)s")
    }

    __LOG_LEVEL = ["CRITICAL", "FATAL", "ERROR", "WARNING", "WARN", "INFO", "DEBUG", "NOTSET"]
    __LOG_LEVEL_DEFAULT = "INFO"

    __TO_INT_LIST = ("port", "timeout", "max_connections")

    __ip_re = re.compile(r"^((25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d)))\.){3}(25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d)))$")
    __url_re = re.compile("^[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]$")

    def __new__(cls, cfg_file="config.ini", *args, **kwargs):
        cls.__load_cfg(cfg_file)
        cls.__set_cfg()
        return cls

    @classmethod
    def __check_sections(cls, cfg):
        sections = cfg.sections()
        if not all(map(lambda x: x.islower(), sections)):
            raise CFGError("配置文件主项请一致使用小写")
        needs = ["mysql", "redis", "coin", "rpc"]
        if not all([s in sections for s in needs]):
            raise CFGError("配置文件中必须包含基本信息")

    @classmethod
    def __load_cfg(cls, cfg_file):
        if not os.path.isfile(cfg_file):
            raise CFGError("请检查文件正确性")

        # 使用RawConfigParser，否则不能识别%
        cls.__cfg = cfg = configparser.RawConfigParser(allow_no_value=True)
        cfg.read(cfg_file, encoding='utf-8')
        cls.__check_sections(cfg)

    @classmethod
    def __set_cfg(cls):
        for k, v in cls.__cfg.items():
            if not k or not k.islower():
                continue
            setattr(cls, k, type(k, (object, ), {}))
            cls.__set_attr(k, v)

    @classmethod
    def __set_attr(cls, obj, opt):
        def to_int(k, dig):
            if k in cls.__TO_INT_LIST:
                if isinstance(dig, (bytes)):
                    dig = dig.decode()
                if isinstance(dig, (str,)):
                    if not dig.isdigit():
                        raise CFGError("端口必须为数字")
                dig = int(dig)
            return dig
        opts = {k: to_int(k, v) for k, v in opt.items()}
        single_cfgs = cls.__INTEGRANT.get(obj)
        if not single_cfgs:
            setattr(cls, obj, type(obj, (object, ), opts))
            setattr(getattr(cls, obj), obj + "_dict", opts)
            return
        for k, v in single_cfgs.items():
            # 检测host正确性
            if k == "host":
                if not cls.__ip_re.match(opts.get(k, v)) and not cls.__url_re.match(opt.get(k, v)):
                    raise CFGError("IP或URL地址不正确.")
            x = opts.setdefault(k, v)
            if x is None:
                raise CFGError("{}缺少基础配置{}".format(obj, k))
        setattr(cls, obj, type(obj, (object,), opts))
        setattr(getattr(cls, obj), obj + "_dict", opts)


if __name__ == "__main__":
    cfg = CFG()
    print(id(cfg), type(cfg))
