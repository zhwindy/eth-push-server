#!/usr/bin/env python
# @Time    : 18-10-15 上午9:40
# @Author  : WangYiqi
# @File    : doge.py
# @DESC    : doge交易数据解析插件

from lib.iplugin import TxPlugin
from coin.btc.base import BtcParserBase

__all__ = ["Doge"]


class DogeParser(BtcParserBase):
    """doge 解析处理器"""

    def __init__(self):
        super().__init__()
        self.coin_type = "DOGE"
        self.rollback = False
        self.rollback_count = 0

    def get_address_magicbyte(self, address_type='P2PKH'):
        """
        返回生成地址的版本前缀
        """
        magicbyte_length = 1
        address_type = address_type.upper()
        if address_type == 'P2PKH':
            magicbyte = 30
        elif address_type == 'P2SH':
            magicbyte = 22
        else:
            magicbyte = 0

        return magicbyte, magicbyte_length


class Doge(TxPlugin):
    name = "doge"
    desc = "解析doge数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = DogeParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        self.parser.rollback = rollback
        self.parser.rollback_count = rollback_count
        return self.parser.parse_block(block_num)

    def push_mempool_list(self, redis_mempool):
        return self.parser.parse_mempool_info(redis_mempool)