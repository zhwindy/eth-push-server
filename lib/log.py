#!/usr/bin/env python

import logging

from logging.handlers import RotatingFileHandler


class Log:
    def __new__(cls, cfg, *args, **kwargs):
        level = cfg.log.log_dict.get("level")
        fmt = cfg.log.log_dict.get("format")
        filename = cfg.log.log_dict.get("filename")
        output = cfg.log.log_dict.get("output")
        logger = logging.getLogger()
        logger.setLevel(level)
        fmt = logging.Formatter(fmt)
        # 设置CMD日志
        if output == "cmd":
            sh = logging.StreamHandler()
            sh.setFormatter(fmt)
            sh.setLevel(level)
            logger.addHandler(sh)
        # 设置文件日志
        if output == "file":
            # 每个日志文件20M,最多保留10个日志文件
            fh = RotatingFileHandler(filename, maxBytes=20 * 1024 * 1024, backupCount=10)
            fh.setFormatter(fmt)
            fh.setLevel(level)
            logger.addHandler(fh)
        return logger
