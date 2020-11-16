#!/usr/bin/env python
import json
from functools import partial
from db import mysql, redis_db, kafka
from core.globals import G_LOGGER
from lib.tool import Single, CachedProperty, seperate_big_list


class Db(Single):
    __first_init = False

    def __init__(self, cfg):
        # 只实例化一次，多次的情况下，mq会出现卡死的情况
        if not Db.__first_init:
            self.cfg = cfg
            self.coin = cfg.coin.coin_dict.get("name")
            # self.mysql = mysql.MySQLDB(cfg)
            self.redis = redis_db.RedisDB(cfg)
            # self.kafka = kafka.KafkaQueue(cfg)
            Db.__first_init = True

    def get_basic(self, coin_name):
        """
        获取basic的信息
        """
        basic = {}
        basic_info = self.redis.get_basic()
        if basic_info:
            basic["current_height"] = basic_info["current_height"]
            basic["newest_height"] = basic_info["newest_height"]
        return basic

    def update_basic(self, info):
        """
        更新basic信息
        """
        self.redis.update_basic(info)