# -*- encode: utf-8 -*-
# Author: Arthur.Gao -- 黑小帅

from lib.iplugin import TxPlugin
from coin.htdf.base import HtdfParser

__all__ = ['Htdf']


class Htdf(TxPlugin):
    name = "htdf"
    desc = "解析htdf数据"
    version = "0.0.1"

    def __init__(self):
        super().__init__()
        self.parser = HtdfParser(self.name)

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num):
        return self.parser.parse_block(block_num)
