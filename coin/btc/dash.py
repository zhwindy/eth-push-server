#!/usr/bin/env python
# @Time    : 18-10-17 下午14:40
# @Author  : zhaopengfei

from lib.iplugin import TxPlugin
from coin.btc.base import BtcParserBase

__all__ = ["Dash"]


class DashParser(BtcParserBase):
    """dash 解析处理器"""

    def __init__(self):
        super().__init__()
        self.coin_type = "DASH"
        self.rollback = False
        self.rollback_count = 0

    def get_address_magicbyte(self, address_type='P2PKH'):
        """
        返回生成地址的版本前缀
        """
        magicbyte_length = 1
        address_type = address_type.upper()
        if address_type == 'P2PKH':
            magicbyte = 76
        elif address_type == 'P2SH':
            magicbyte = 16
        else:
            magicbyte = 0

        return magicbyte, magicbyte_length


class Dash(TxPlugin):
    name = "dash"
    desc = "解析dash交易数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = DashParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        self.parser.rollback = rollback
        self.parser.rollback_count = rollback_count
        return self.parser.parse_block(block_num)

    def push_mempool_list(self, redis_mempool):
        return self.parser.parse_mempool_info(redis_mempool)