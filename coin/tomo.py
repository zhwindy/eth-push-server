#!/usr/bin/env python
# @Time    : 19-11-15 上午11:33
# @Author  : Huyongqiao
# @File    : tomo.py
# @DESC    : tomo（TOMO币）交易数据解析插件

import time
import os
import sys
from lib.iplugin import TxPlugin
from rpc.jsonrpc import JsonRpcRequest
from core.globals import G_CFG, G_PUSH_TEMPLATE, G_LOGGER

__all__ = ["Tomo"]


class TomoRpc(JsonRpcRequest):
    """tomo rpc接口"""
    def __init__(self, cfg):
        super().__init__(cfg)

    def get_block_count(self):
        """
        获取最新高度
        :return:
        """
        method = "eth_blockNumber"
        return self._single_post(method)

    def get_block(self, block_num):
        """
        根据区块高度获取区块详情
        :param block_num: 区块高度
        :return:
        """
        method = "eth_getBlockByNumber"
        params = [block_num, True]
        return self._single_post(method, params)

    def get_tx_receipt(self, tx_hash):
        """
        获取交易收据，里面包含交易状态和使用的gas值
        :param tx_hash: 交易哈希值
        :return:
        """
        method = "eth_getTransactionReceipt"
        params = [tx_hash]
        return self._single_post(method, params)


class TomoParser:
    """tomo解析处理器"""
    def __init__(self):
        self.rpc = TomoRpc(G_CFG)
        self.rollback = False
        self.rollback_count = 0

    def newest_height(self):
        """
        获取最新区块高度
        :return:
        """
        height = self.rpc.get_block_count()
        return int(height, 16)

    def parse_block(self, block_num):
        """
        解析区块的交易详情
        :return: 返回待推送信息列表
        """
        while True:
            try:
                if isinstance(block_num, int):
                    block_num = hex(block_num)
                if isinstance(block_num, str) and not block_num.startswith('0x'):
                    block_num = hex(int(block_num))
                block = self.rpc.get_block(block_num)
                txs = block.get("transactions", [])
                push_list = []
                for tx in txs:
                    tx_receipt = self.rpc.get_tx_receipt(tx.get("hash"))
                    mq_tx = G_PUSH_TEMPLATE.copy()
                    mq_tx["Txid"] = tx.get("hash")
                    mq_tx["Type"] = "TOMO"
                    mq_tx["From"] = tx.get("from")
                    mq_tx["To"] = tx.get("to")
                    mq_tx["Amount"] = int(tx.get("value"), 16)
                    mq_tx["Fee"] = int(tx["gasPrice"], 16) * int(tx_receipt["gasUsed"], 16)
                    status = int(tx_receipt["status"], 16)
                    mq_tx["Valid"] = True if status else False
                    mq_tx["status"] = "true" if status else "false"
                    mq_tx["Time"] = int(block.get("timestamp"), 16)
                    mq_tx["BlockNumber"] = int(tx.get("blockNumber"), 16)
                    push_list.append(mq_tx)
                return push_list
            except Exception as e:
                G_LOGGER.info(f"出现异常，尝试重新获取。异常原因：{str(e)}")
                time.sleep(1)


class Tomo(TxPlugin):

    name = "tomo"
    desc = "解析tomo数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = TomoParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        return self.parser.parse_block(block_num)


if __name__ == "__main__":
    import json
    e = Tomo()
    b = e.newest_height()
    print(b)
    b = 12922108
    r = e.push_list(b)
    print(json.dumps(r))
