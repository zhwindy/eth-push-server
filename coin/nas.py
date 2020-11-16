#!/usr/bin/env python
# @Time    : 2019-5-10 9:36
# @Author  : Humingxing
# @File    : nas.py
# @DESC    : nas（星云链）交易数据解析插件

from lib.tool import full_url
from lib.iplugin import TxPlugin
from rpc.jsonrpc import HttpMixin
from core.globals import G_CFG, G_PUSH_TEMPLATE

__all__ = ["Nas"]


class NasRpc(HttpMixin):
    """Nas http接口"""
    def __init__(self, cfg):
        timeout = cfg.rpc.rpc_dict.get("timeout")
        super().__init__(timeout)
        self.url = cfg.rpc.rpc_dict["url"]
        self.set_json()

    def get_block_count(self):
        """
        获取最新高度
        :return:
        """
        url = full_url(self.url, "/v1/user/lib")
        return self._single_get(url)

    def get_block(self, block_num):
        """
        根据区块高度获取区块详情
        :param block_num: 区块高度
        :return:
        """
        url = full_url(self.url, "/v1/user/getBlockByHeight")
        params = {"height": block_num, "full_fill_transaction": True}
        return self._single_post(url, params=params)


class NasParser:
    """nas解析处理器"""
    def __init__(self):
        self.rpc = NasRpc(G_CFG)

    def newest_height(self):
        """
        获取最新区块高度
        :return:
        """
        block = self.rpc.get_block_count()
        return int(block["result"]["height"])

    def parse_block(self, block_num):
        """
        解析区块的交易列表
        :return: 返回待推送信息列表
        """
        block = self.rpc.get_block(block_num)
        txs = block["result"]["transactions"] if block else []

        tx_index = 0
        push_list = []
        for tx in txs:
            if tx.get("type") != "binary" or tx.get("status") != 1:
                continue
            mq_tx = G_PUSH_TEMPLATE.copy()
            mq_tx["BlockNumber"] = tx["block_height"]
            mq_tx["Time"] = tx["timestamp"]
            mq_tx["Txid"] = tx["hash"]
            mq_tx["Type"] = "NAS"
            mq_tx["From"] = tx["from"]
            mq_tx["To"] = tx["to"]
            mq_tx["Amount"] = tx["value"]
            mq_tx["TxIndex"] = tx_index
            push_list.append(mq_tx)
            tx_index += 1
        return push_list


class Nas(TxPlugin):

    name = "nas"
    desc = "解析nas数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = NasParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num):
        return self.parser.parse_block(block_num)
