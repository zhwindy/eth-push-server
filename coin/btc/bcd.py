#!/usr/bin/env python
from lib.iplugin import TxPlugin
from coin.btc.base import BtcParserBase

__all__ = ["Bcd"]


class BcdParser(BtcParserBase):
    """
    bcd解析处理器
    """
    def __init__(self):
        super().__init__()
        self.coin_type = "BCD"
        # 币数量单位小数点数位
        self.coin_value_unit = 7
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


class Bcd(TxPlugin):

    name = "bcd"
    desc = "解析bcd数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = BcdParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        self.parser.rollback = rollback
        self.parser.rollback_count = rollback_count
        return self.parser.parse_block(block_num)

    def push_mempool_list(self, redis_mempool):
        return self.parser.parse_mempool_info(redis_mempool)
