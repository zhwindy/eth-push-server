#!/usr/bin/env python
# @Time    : 18-10-17 上午9:40
# @Author  : zhaopengfei

from lib.iplugin import TxPlugin
from coin.btc.base import BtcParserBase

__all__ = ["Qtum"]


class QtumParser(BtcParserBase):
    """qtum 解析处理器"""

    def __init__(self):
        super().__init__()
        self.coin_type = "QTUM"
        self.rollback = False
        self.rollback_count = 0

    def get_address_magicbyte(self, address_type='P2PKH'):
        """
        返回生成地址的版本前缀
        """
        magicbyte_length = 1
        address_type = address_type.upper()
        if address_type == 'P2PKH':
            magicbyte = 58
        elif address_type == 'P2SH':
            magicbyte = 50
        else:
            magicbyte = 58

        return magicbyte, magicbyte_length


class Qtum(TxPlugin):
    name = "qtum"
    desc = "解析qtum交易数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = QtumParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        self.parser.rollback = rollback
        self.parser.rollback_count = rollback_count
        return self.parser.parse_block(block_num)

    def push_mempool_list(self, redis_mempool):
        return self.parser.parse_mempool_info(redis_mempool)