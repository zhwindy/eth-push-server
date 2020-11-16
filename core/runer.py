#!/usr/bin/env python
import os
import time
import json
import traceback
import requests
from db.dbs import Db
from lib.email import Email
from lib.error import SyncError, ForkError
from lib.tool import get_now, loop_wrap, Single
from core.globals import G_LOGGER, G_CFG
from decimal import Decimal


class CoinPush(Single):

    block_num = None

    def __init__(self, coin):
        super().__init__()
        self.coin = coin
        self.coin_name = G_CFG.coin.coin_dict.get("name")
        self.process = G_CFG.coin.coin_dict.get("process")
        self.db = Db(G_CFG)
        self.redis_conn = self.db.redis.client
        self.mail = Email(G_CFG)
        self.mode = G_CFG.coin.coin_dict["mode"]
        self.rollback_count = None

    def kafka_push(self, data):
        try:
            # 兼容处理eth_multi和其他币种两套数据模板
            if "Type" in data:
                # 把空字符串、None值转化为0，避免eval处理出错
                if not data["Amount"]:
                    data["Amount"] = 0
                if not data["Fee"]:
                    data["Fee"] = 0
                if not data["BlockNumber"]:
                    data["BlockNumber"] = 0
                if not data["Time"]:
                    data["Time"] = 0

                # eval可以把小数型字符串和16进制字符串转换为小数和整数类型
                if isinstance(data["Amount"], str):
                    data["Amount"] = eval(data["Amount"])
                if isinstance(data["Fee"], str):
                    data["Fee"] = eval(data["Fee"])
                if isinstance(data["BlockNumber"], str):
                    data["BlockNumber"] = eval(data["BlockNumber"])
                if isinstance(data["Time"], str):
                    data["Time"] = eval(data["Time"])

                # 根据每个币种精度，转化为整型数值
                if data["Type"] in ["EOS"]:
                    data["Amount"] = Decimal(str(data["Amount"])) * pow(10, 4)
                    data["Fee"] = Decimal(str(data["Fee"])) * pow(10, 4)
                if data["Type"] in ["IOST"]:
                    data["Amount"] = Decimal(str(data["Amount"])) * pow(10, 8)
                    data["Fee"] = Decimal(str(data["Fee"])) * pow(10, 8)

                # 去除小数位无效的0
                data["Amount"] = int(data["Amount"])
                data["Fee"] = int(data["Fee"])
                data["BlockNumber"] = int(data["BlockNumber"])
                data["Time"] = int(data["Time"]) * 1000
            else:
                data["time"] = eval(data["time"]) * 1000
                data["value"] = eval(data["value"])
                data["Fee"] = eval(data["Fee"])

            partition, offset = self.db.kafka.send(data)
            G_LOGGER.info("Process:{} kafka push success, partition={}, offset={}, push_data={}".format(os.getpid(), partition, offset, data))
        except Exception as e:
            G_LOGGER.error("Process:{} kafka push failed, push_data={}, error={}".format(os.getpid(), data, str(e)))

    def rocket_push(self, data):
        try:
            # 兼容处理eth_multi和其他币种两套数据模板
            if "Type" in data:
                # 把空字符串、None值转化为0，避免eval处理出错
                if not data["Amount"]:
                    data["Amount"] = 0
                if not data["Fee"]:
                    data["Fee"] = 0
                if not data["BlockNumber"]:
                    data["BlockNumber"] = 0
                if not data["Time"]:
                    data["Time"] = 0

                # eval可以把小数型字符串和16进制字符串转换为小数和整数类型
                if isinstance(data["Amount"], str):
                    data["Amount"] = eval(data["Amount"])
                if isinstance(data["Fee"], str):
                    data["Fee"] = eval(data["Fee"])
                if isinstance(data["BlockNumber"], str):
                    data["BlockNumber"] = eval(data["BlockNumber"])
                if isinstance(data["Time"], str):
                    data["Time"] = eval(data["Time"])

                # 根据每个币种精度，转化为整型数值
                if data["Type"] in ["EOS"]:
                    data["Amount"] = Decimal(str(data["Amount"])) * pow(10, 4)
                    data["Fee"] = Decimal(str(data["Fee"])) * pow(10, 4)
                if data["Type"] in ["IOST"]:
                    data["Amount"] = Decimal(str(data["Amount"])) * pow(10, 8)
                    data["Fee"] = Decimal(str(data["Fee"])) * pow(10, 8)

                # 去除小数位无效的0
                data["Amount"] = int(data["Amount"])
                data["Fee"] = int(data["Fee"])
                data["BlockNumber"] = int(data["BlockNumber"])
                data["Time"] = int(data["Time"]) * 1000
            else:
                data["time"] = eval(data["time"]) * 1000
                data["value"] = eval(data["value"])
                data["Fee"] = eval(data["Fee"])
            msg_id = self.db.rocket.push_data(data)
            G_LOGGER.info("Process:{} kafka push success, msg_id={}, push_data={}".format(os.getpid(), msg_id, data))
        except Exception as e:
            G_LOGGER.error("Process:{} kafka push failed, push_data={}, error={}".format(os.getpid(), data, str(e)))

    def push_sync(self, height, num, rollback_count=0):
        block_num = height
        try:
            for block_num in range(height, height + num):
                self.block_num = block_num
                # 获取待推送数据
                push_data = self.coin.push_list(block_num, rollback_count=rollback_count)
                G_LOGGER.info("当前推送区块高度为:{}, push_counts={}".format(self.block_num, len(push_data)))
                for data in push_data:
                    self.rocket_push(data)
                time.sleep(0.5)
        except ForkError as e:
            raise ForkError(e.height, e.msg)
        except Exception as e:
            # error_info = traceback.format_exc()
            error_info = str(e)
            raise SyncError(block_num, f"{block_num}{str(error_info)}")

    def push_main(self, save_redis=False):
        """
        推送处理
        :param save_redis: 待处理的区块先存入redis，先不推送
        1. 默认save_redis为False直接推送
        2. 为True则将待处理区块存入redis
        """
        if not self.coin_name:
            raise Exception("unknow coin name")
        basic_info = self.db.get_basic(self.coin_name)
        if not basic_info:
            G_LOGGER.info("币种{}没有相关配置,请检查数据库配置".format(self.coin_name))
            return True

        save_num = current_height = basic_info["current_height"]
        if save_redis:
            newest_block_height = self.coin.newest_height()
            diff_num = newest_block_height - current_height
        else:
            newest_block_height = self.coin.newest_height()
            diff_num = newest_block_height - current_height
        if diff_num <= 0:
            G_LOGGER.info("最新高度{},已同步高度{},无需推送".format(newest_block_height, current_height))
            return True
        try:
            if save_redis:
                step = 100  # 最大处理100个块存入redis
                diff_num = step if diff_num > step else diff_num
                G_LOGGER.info("当前最新高度为{}, 数据库中的高度{}, 本次需要放入redis的块数为{}".format(newest_block_height, current_height, diff_num))
                # 待推送的区块高度存入到redis队列中
                for height in range(current_height + 1, current_height + diff_num + 1):
                    self.block_num = height
                    G_LOGGER.info("redis_save_pending_block: {}".format(self.block_num))
                    self.db.redis.save_pending_block(self.block_num)
            else:
                diff_num = 10
                G_LOGGER.info("最新高度{},已同步高度{},本次需要同步块数{}".format(newest_block_height, current_height, diff_num))
                self.push_sync(current_height + 1, diff_num, rollback_count=self.rollback_count)
        except ForkError as e:
            G_LOGGER.error("同步过程出现临时分叉, 即将回滚至高度{}重新推送".format(e.height))
            save_num = e.height
        except SyncError as e:
            err_info = traceback.format_exc()
            G_LOGGER.error("{}块同步过程出现异常, 请处理此块. 详情: {}".format(e.height, str(err_info)))
        except Exception as e:
            G_LOGGER.error("同步过程出现异常, 请处理此块. 详情{}".format(e))
        except KeyboardInterrupt:
            G_LOGGER.info("同步过程手动取消任务, 同步至高度: {}. 已保存".format(self.block_num))
            save_num = self.block_num
        else:
            save_num = self.block_num
        finally:
            basic_info['current_height'] = save_num
            basic_info['newest_height'] = newest_block_height
            self.db.update_basic(basic_info)
            G_LOGGER.info("push success ,已同步保存至区块高度: {}".format(save_num))
        return True

    def push_mempool_info(self):
        """
        推送mempool中的未确认交易信息
        """
        redis_mempool_list = self.db.redis.get_mempool_info()
        G_LOGGER.info('Mempool中未确认交易数量:{}'.format(len(redis_mempool_list)))
        try:
            old_tx_ids, new_tx_ids, push_data = self.coin.push_mempool_list(redis_mempool_list)
            for data in push_data:
                self.kafka_push(data)
            update_redis_mempool_list = [tx_id for tx_id in redis_mempool_list if tx_id not in old_tx_ids]
            update_redis_mempool_list.extend(new_tx_ids)
            self.db.redis.update_mempool_info(update_redis_mempool_list)
            # 避免频繁请求节点,每隔6秒检测mempool
            time.sleep(6)
        except Exception:
            msg = traceback.format_exc()
            G_LOGGER.info("推送MemPool信息出错, 错误: {}.".format(msg))

    @loop_wrap
    def loop_push(self):
        """
        块内tx推送
        """
        save_redis = True if int(self.process) > 1 else False
        self.push_main(save_redis=save_redis)

    @loop_wrap
    def loop_mempool(self):
        """
        Mempool推送
        """
        self.push_mempool_info()

    def loop_fetch_block(self):
        """
        从redis取出待处理的块进行解析
        """
        while True:
            block_num = self.db.redis.get_pending_block()
            if len(block_num) > 0:
                block_num = block_num[0].decode("utf-8")
                diff_num = 1
                G_LOGGER.info(f"进程{os.getpid()}正在处理{block_num}区块")
                try:
                    self.push_sync(int(block_num), diff_num)
                except Exception as e:
                    self.db.redis.save_pending_block(block_num)
                    # err_info = traceback.format_exc()
                    err_info = str(e)
                    G_LOGGER.info(f"进程{os.getpid()}正在处理{block_num}区块异常, 详情::{err_info}")
            time.sleep(0.1)
