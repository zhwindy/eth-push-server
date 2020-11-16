#!/usr/bin/env python
import json
from functools import partial
from db import mysql, redis_db, mq, kafka
from core.globals import G_LOGGER
from lib.tool import Single, CachedProperty, seperate_big_list


class Db(Single):
    __first_init = False

    def __init__(self, cfg):
        # 只实例化一次，多次的情况下，mq会出现卡死的情况
        if not Db.__first_init:
            self.cfg = cfg
            self.coin = cfg.coin.coin_dict.get("name")
            self.mysql = mysql.MySQLDB(cfg)
            # self.mq = mq.RabbitQueue(cfg)
            self.redis = redis_db.RedisDB(cfg)
            self.kafka = kafka.KafkaQueue(cfg)
            Db.__first_init = True

    def get_basic(self, coin_name):
        """
        获取basic的信息
        """
        mysql_basic = {}
        try:
            basic_info = self.mysql.basic.get_basic(coin_name)
            if basic_info:
                mysql_basic["id"] = int(basic_info.id)
                mysql_basic["current_height"] = basic_info.current_height
                mysql_basic["newest_height"] = basic_info.newest_height
                mysql_basic["rollback_count"] = basic_info.rollback_count
        except Exception as e:
            G_LOGGER.error(f"mysql获取basic出错，出错原因:{str(e)}")

        return mysql_basic

    def update_basic(self, item):
        """
        更新basic的信息
        """
        try:
            self.mysql.basic.update_basic(item["id"], item["current_height"], item["newest_height"])
        except Exception as e:
            G_LOGGER.error(f"mysql更新basic信息出错, 出错原因:{str(e)}")

    def save_transaction(self, coin_name, transactions):
        """
        保存重新推送的tx
        """
        try:
            transaction_data = ["('{}', {}, '{}')".format(coin_name, tx['BlockNumber'], tx['Txid']) for tx in transactions]
            for txs in seperate_big_list(transaction_data):
                self.mysql.transaction.save_transactions(txs)
        except Exception as e:
            G_LOGGER.error(f"mysql保存tracsaction时出错, 出错原因:{str(e)}")

    def save_log(self, data):
        """
        保存失败日志
        """
        try:
            self.mysql.log.insert_log(data)
        except Exception as e:
            G_LOGGER.info(f"保存日志到mysql失败，失败原因:{str(e)}，已保存至redis，数据：{data}")

    @CachedProperty
    def mq_post(self):
        exchange_name = self.cfg.mq.mq_dict.get("exchange_name")
        v_host = self.cfg.mq.mq_dict.get("vhost")
        routing_key = self.cfg.mq.mq_dict.get("routing_key")
        return partial(self.mq.rab_post_message, e_name=exchange_name, v_host=v_host, routing_key=routing_key)
