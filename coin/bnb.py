#!/usr/bin/env python
# @Time    : 19-12-3 上午11:30
# @Author  : Huyongqiao
# @File    : bnb.py
# @DESC    : bnb（BNB币）交易数据解析插件

import time
import os
import sys
from lib.iplugin import TxPlugin
from rpc.httplib import HttpMixin
from rpc.jsonrpc import JsonRpcRequest
from core.globals import G_CFG, G_PUSH_TEMPLATE, G_LOGGER
from lib.tool import full_url, utcstr_to_timestamp

__all__ = ["Bnb"]


class BnbRpc(HttpMixin):
    """bnb rpc接口"""
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
        url = full_url(self.url, "/abci_info")
        return self._single_get(url)

    def get_block(self, block_num):
        """
        根据区块高度获取区块详情
        :param block_num: 区块高度
        :return:
        """
        url = 'https://dex.binance.org/api/v1/transactions-in-block/{}'.format(block_num)
        return self._single_get(url)


class BnbParser:
    """bnb解析处理器"""
    def __init__(self):
        self.rpc = BnbRpc(G_CFG)
        self.rollback = False
        self.rollback_count = 0

    def newest_height(self):
        """
        获取最新区块高度
        :return:
        """
        res = self.rpc.get_block_count()
        return int(res["result"]["response"]["last_block_height"])

    def parse_block(self, block_num):
        """
        解析区块的交易详情
        :return: 返回待推送信息列表
        """
        while True:
            try:
                block = self.rpc.get_block(block_num)
                txs = block.get("tx", [])
                push_list = []
                for tx in txs:
                    G_LOGGER.info("tx={}".format(tx))
                    mq_tx = G_PUSH_TEMPLATE.copy()
                    mq_tx["Txid"] = tx.get("txHash")
                    mq_tx["Type"] = "BNB"
                    mq_tx["From"] = tx.get("fromAddr")
                    mq_tx["To"] = tx.get("toAddr")
                    mq_tx["Amount"] = int(float(tx.get("value") if tx.get("value") else 0) * 100000000)
                    mq_tx["Fee"] = int(float(tx.get("txFee") if tx.get("txFee") else 0) * 100000000)
                    mq_tx["Valid"] = True
                    mq_tx["Time"] = utcstr_to_timestamp(tx.get("timeStamp")) if tx.get("timeStamp") else 0
                    mq_tx["BlockNumber"] = tx.get("blockHeight")
                    mq_tx["TxIndex"] = 0
                    mq_tx["Memo"] = tx.get("memo") if tx.get("memo") else ""
                    G_LOGGER.info("mq_tx={}".format(mq_tx))
                    push_list.append(mq_tx)
                return push_list
            except Exception as e:
                G_LOGGER.info(f"出现异常，尝试重新获取。异常原因：{str(e)}")
                time.sleep(1)


class Bnb(TxPlugin):

    name = "bnb"
    desc = "解析bnb数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = BnbParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        return self.parser.parse_block(block_num)


if __name__ == "__main__":
    import json
    e = Bnb()
    b = e.newest_height()
    print(b)
    b = 50799653
    r = e.push_list(b)
    print(json.dumps(r))
