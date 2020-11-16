#!/usr/bin/env python
# @Time    : 18-10-16 下午15:45
# @Author  : WangYiqi
# @File    : sbtc.py
# @DESC    : sbtc交易数据解析插件
import time
from coin.btc.base import BtcParserBase
from core.globals import G_PUSH_TEMPLATE
from lib.iplugin import TxPlugin

__all__ = ["Sbtc"]


class SbtcParser(BtcParserBase):
    """sbtc 解析处理器"""

    def __init__(self):
        super().__init__()
        self.coin_type = "SBTC"
        self.rollback = False
        self.rollback_count = 0

    def get_address_magicbyte(self, address_type='P2PKH'):
        """
        返回生成地址的版本前缀
        """
        magicbyte_length = 1
        address_type = address_type.upper()
        if address_type == 'P2PKH':
            magicbyte = 0
        elif address_type == 'P2SH':
            magicbyte = 5
        else:
            magicbyte = 0

        return magicbyte, magicbyte_length


class Sbtc(TxPlugin):
    name = "sbtc"
    desc = "解析sbtc数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = SbtcParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        self.parser.rollback = rollback
        self.parser.rollback_count = rollback_count
        return self.parser.parse_block(block_num)

    def push_mempool_list(self, redis_mempool):
        return self.parser.parse_mempool_info(redis_mempool)