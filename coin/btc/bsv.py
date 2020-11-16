#!/usr/bin/env python
from lib.iplugin import TxPlugin
from core.globals import G_PUSH_TEMPLATE
from coin.btc.base import BtcParserBase

__all__ = ["Bsv"]


class BsvParser(BtcParserBase):
    """
    bsv解析处理器
    """
    def __init__(self):
        super().__init__()
        self.coin_type = "BSV"
        self.rollback = False
        self.rollback_count = 0
        # 每次从节点请求的tx笔数
        self.request_chunk_size = 5

    # def get_address_magicbyte(self, address_type='P2PKH'):
    #     """
    #     返回生成地址的版本前缀
    #     """
    #     magicbyte_length = 1
    #     address_type = address_type.upper()
    #     if address_type == 'P2PKH':
    #         magicbyte = 28
    #     elif address_type == 'P2SH':
    #         magicbyte = 40
    #     else:
    #         magicbyte = 0

    #     return magicbyte, magicbyte_length


class Bsv(TxPlugin):

    name = "bsv"
    desc = "解析bsv数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = BsvParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        self.parser.rollback = rollback
        self.parser.rollback_count = rollback_count
        return self.parser.parse_block(block_num)

    def push_mempool_list(self, redis_mempool):
        return self.parser.parse_mempool_info(redis_mempool)
