#!/usr/bin/env python
# @Time    : 18-10-17 上午9:40
# @Author  : zhaopengfei

from lib.iplugin import TxPlugin
from coin.btc.base import BtcParserBase

__all__ = ["Zec"]


class ZecParser(BtcParserBase):
    """zec 解析处理器"""

    def __init__(self):
        super().__init__()
        self.coin_type = "ZEC"
        self.rollback = False
        self.rollback_count = 0

    def get_address_magicbyte(self, address_type='P2PKH'):
        """
        返回生成地址的版本前缀
        """
        magicbyte_length = 2
        address_type = address_type.upper()
        if address_type == 'P2PKH':
            magicbyte = 7352
        elif address_type == 'P2SH':
            magicbyte = 7357
        else:
            magicbyte = 0

        return magicbyte, magicbyte_length


class Zec(TxPlugin):
    name = "zec"
    desc = "解析zec交易数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = ZecParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        self.parser.rollback = rollback
        self.parser.rollback_count = rollback_count
        return self.parser.parse_block(block_num)

    def push_mempool_list(self, redis_mempool):
        return self.parser.parse_mempool_info(redis_mempool)


if __name__ == "__main__":
    import json
    e = Zec()
    b = 608977
    r = e.push_list(b)
    print(json.dumps(r))
