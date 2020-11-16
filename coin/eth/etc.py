#!/usr/bin/env python
# @Time    : 18-10-9 下午5:40
# @Author  : Humingxing
# @File    : etc.py
# @DESC    : etc交易数据解析插件(etc rpc接口目前和eth的一致，所以代码处理上也是一样的，但还是单独创建etc插件，方便以后扩展和修改)

from lib.iplugin import TxPlugin
from coin.eth.base import EtParser

__all__ = ["Etc"]


class Etc(TxPlugin):

    name = "etc"
    desc = "解析etc数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = EtParser("ETC")

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        self.parser.rollback = rollback
        self.parser.rollback_count = rollback_count
        return self.parser.parse_block(block_num)


if __name__ == "__main__":
    e = Etc()
    r = e.push_list(8709888)
    import json
    print(json.dumps(r))
