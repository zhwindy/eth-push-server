#!/usr/bin/env python
# @Time    : 18-11-15 上午10:05
# @Author  : Humingxing
# @File    : btm.py
# @DESC    : btm（比原链）交易数据解析插件

from lib.tool import full_url
from lib.iplugin import TxPlugin
from rpc.jsonrpc import HttpMixin
from core.globals import G_CFG, G_PUSH_TEMPLATE

__all__ = ["Btm"]


class BtmRpc(HttpMixin):
    """btm http接口"""
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
        url = full_url(self.url, "get-block-count")
        return self._single_get(url)

    def get_block(self, block_num):
        """
        根据区块高度获取区块详情
        :param block_num: 区块高度
        :return:
        """
        url = full_url(self.url, "get-block")
        params = {"block_height": block_num}
        return self._single_post(url, params)


class BtmParser:
    """btm解析处理器"""
    def __init__(self):
        self.rpc = BtmRpc(G_CFG)
        self.rollback = False
        self.rollback_count = 0

    def newest_height(self):
        """
        获取最新区块高度
        :return:
        """
        result = self.rpc.get_block_count()
        return result["data"]["block_count"]

    def parse_block(self, block_num):
        """
        解析区块的交易列表
        :return: 返回待推送信息列表
        """
        block = self.rpc.get_block(block_num)
        txs = block["data"]["transactions"]
        push_list = []
        for tx in txs:
            mq_tx = G_PUSH_TEMPLATE.copy()
            mq_tx["BlockNumber"] = block["data"]["height"]
            mq_tx["Time"] = block["data"]["timestamp"]
            mq_tx["Txid"] = tx["id"]
            mq_tx["Type"] = "BTM"
            vins = tx["inputs"]
            is_coinbase = False  # 是否挖矿交易
            input_amount = 0
            for i, vin in enumerate(vins):
                input_amount += vin.get("amount", 0)
                if vin["type"] == "coinbase":
                    is_coinbase = True
                    break
                if i == 0:
                    # 取第一个input中的address作为from地址
                    mq_tx["From"] = vin["address"] if "address" in vin.keys() else ""
            vouts = tx["outputs"]
            output_amount = 0
            for i, vout in enumerate(vouts):
                output_amount += vout.get("amount", 0)
                # 如果是挖矿交易,且交易类型不为control则不推送
                if is_coinbase or vout['type'] != 'control' or "address" not in vout.keys():
                    continue
                mq_tx = mq_tx.copy()
                mq_tx["To"] = vout["address"]
                mq_tx["Amount"] = vout["amount"]
                mq_tx["Contract"] = vout["asset_id"]
                mq_tx["VoutsIndex"] = i
                mq_tx["status"] = "true"
                push_list.append(mq_tx)
            for _push in push_list:
                _push["Fee"] = input_amount - output_amount
        return push_list


class Btm(TxPlugin):

    name = "btm"
    desc = "解析btm数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = BtmParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        return self.parser.parse_block(block_num)
