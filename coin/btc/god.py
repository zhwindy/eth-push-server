#!/usr/bin/env python
# @Time    : 18-10-19 下午14:47
# @Author  : WangYiqi
# @File    : god.py
# @DESC    : god交易数据解析插件

from coin.btc.base import BtcParserBase
from lib.iplugin import TxPlugin

__all__ = ["God"]


class GodParser(BtcParserBase):
    """god 解析处理器"""

    def __init__(self):
        super().__init__()
        self.coin_type = "GOD"
        self.rollback = False
        self.rollback_count = 0

    def get_address_magicbyte(self, address_type='P2PKH'):
        """
        返回生成地址的版本前缀
        """
        magicbyte_length = 1
        address_type = address_type.upper()
        if address_type == 'P2PKH':
            magicbyte = 97
        elif address_type == 'P2SH':
            magicbyte = 23
        else:
            magicbyte = 0

        return magicbyte, magicbyte_length


class God(TxPlugin):
    name = "god"
    desc = "解析god数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = GodParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        self.parser.rollback = rollback
        self.parser.rollback_count = rollback_count
        return self.parser.parse_block(block_num)

    def push_mempool_list(self, redis_mempool):
        return self.parser.parse_mempool_info(redis_mempool)