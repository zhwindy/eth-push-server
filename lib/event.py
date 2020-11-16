#!/usr/bin/env python
import os
import time
from threading import Thread
from core.runer import CoinPush
from core.globals import G_CFG, G_LOGGER
from lib.plugin import DirectoryPluginManager


class Event:
    """待处理的事件"""

    @classmethod
    def coin_push(cls):
        plugin_manager = DirectoryPluginManager()
        coin_name = G_CFG.coin.coin_dict.get("name")
        plugin_manager.load_plugins(coin_name)
        coin = plugin_manager.get_plugins(coin_name)
        if coin:
            return CoinPush(coin)
        else:
            G_LOGGER.error(f"未安装{coin_name}的交易数据解析插件")
            exit()

    @classmethod
    def push_process(cls):
        """
        交易推送进程
        """
        G_LOGGER.info(f"进程{os.getpid()}启动TX的推送任务")
        cls.coin_push().loop_push()

    @classmethod
    def mempool_process(cls):
        """
        未确认交易推送进程
        """
        # 只有比特系币种才有内存池
        coin_name = G_CFG.coin.coin_dict.get("name")
        btc_serial = ["btc", "bch", "bsv", "bcx", "bcd", "btg", "dash", "doge", "god", "ipc", "ltc", "qtum", "sbtc", "zec", 'usdt']
        if coin_name not in btc_serial:
            return None
        G_LOGGER.info(f"进程{os.getpid()}启动Mempool的推送任务")
        cls.coin_push().loop_mempool()

    @classmethod
    def fetch_process(cls):
        process = G_CFG.coin.coin_dict.get("process")
        if int(process) <= 1:
            return None
        G_LOGGER.info(f"进程{os.getpid()}已开启高并发推送模式")
        cls.coin_push().loop_fetch_block()
