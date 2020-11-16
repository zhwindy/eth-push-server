#!/usr/bin/env python
from coin.btc.base import BtcParserBase
from lib.iplugin import TxPlugin

__all__ = ["Ltc"]


class LtcParser(BtcParserBase):
    """ltc 解析处理器"""

    def __init__(self):
        super().__init__()
        self.coin_type = "LTC"
        self.rollback = False
        self.rollback_count = 0
        # 每次从节点请求的tx笔数
        self.request_chunk_size = 1

    def get_address_magicbyte(self, address_type='P2PKH'):
        """
        返回生成地址的版本前缀
        """
        magicbyte_length = 1
        address_type = address_type.upper()
        if address_type == 'P2PKH':
            magicbyte = 48
        elif address_type == 'P2SH':
            magicbyte = 50
        else:
            magicbyte = 48

        return magicbyte, magicbyte_length


class Ltc(TxPlugin):
    name = "ltc"
    desc = "解析ltc数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = LtcParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        self.parser.rollback = rollback
        self.parser.rollback_count = rollback_count
        return self.parser.parse_block(block_num)

    def push_mempool_list(self, redis_mempool):
        return self.parser.parse_mempool_info(redis_mempool)
