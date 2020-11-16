#!/usr/bin/env python
# @Time    : 18-10-11 下午2:33
# @Author  : zhaopengfei
# @DESC    : BTC交易数据解析插件,BtcParser(解析处理器),Btc(插件接入点)

from lib.iplugin import TxPlugin
from coin.btc.base import BtcParserBase

__all__ = ["Btc"]


class BtcParser(BtcParserBase):
    """
    btc解析处理器
    """
    def __init__(self):
        super().__init__()
        self.coin_type = "BTC"
        self.rollback = False
        self.rollback_count = 0


class Btc(TxPlugin):

    name = "btc"
    desc = "解析btc数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = BtcParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        self.parser.rollback = rollback
        self.parser.rollback_count = rollback_count
        return self.parser.parse_block(block_num)

    def push_mempool_list(self, redis_mempool):
        return self.parser.parse_mempool_info(redis_mempool)
