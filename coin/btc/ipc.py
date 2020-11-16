#!/usr/bin/env python
# @Time    : 18-10-19 下午18:33
# @Author  : WangYiqi
# @File    : ipc.py
# @DESC    : ipc交易数据解析插件
import bitcoin
from coin.btc.base import BtcParserBase
from lib.iplugin import TxPlugin

__all__ = ["Ipc"]


class IpcParser(BtcParserBase):
    """ipc 解析处理器"""

    def __init__(self):
        super().__init__()
        self.coin_type = "IPC"
        self.rollback = False
        self.rollback_count = 0

    def get_address_magicbyte(self, address_type='P2PKH'):
        """
        返回生成地址的版本前缀
        """
        magicbyte_length = 3
        address_type = address_type.upper()
        if address_type == 'P2PKH':
            magicbyte = 269140
        elif address_type == 'P2SH':
            magicbyte = 269140
        else:
            magicbyte = 269140

        return magicbyte, magicbyte_length


class Ipc(TxPlugin):
    name = "ipc"
    desc = "解析ipc数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = IpcParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        self.parser.rollback = rollback
        self.parser.rollback_count = rollback_count
        return self.parser.parse_block(block_num)

    def push_mempool_list(self, redis_mempool):
        return self.parser.parse_mempool_info(redis_mempool)