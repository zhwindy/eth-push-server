#!/usr/bin/env python
# @Time    : 18-10-18 下午3:33
# @Author  : Humingxing
# @File    : nem.py
# @DESC    : nem（新经币）交易数据解析插件

from lib.tool import full_url
from lib.iplugin import TxPlugin
from rpc.httplib import HttpMixin
from core.globals import G_CFG, G_PUSH_TEMPLATE

__all__ = ["Nem"]


class NemHttp(HttpMixin):
    """nem http接口"""
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
        url = full_url(self.url, "/chain/height")
        return self._single_get(url)

    def get_block(self, block_num):
        """
        根据区块高度获取区块详情
        :param block_num: 区块高度
        :return:
        """
        # 该接口一次拿回block_num之后的10个块
        url = full_url(self.url, "/local/chain/blocks-after")
        params = {"height": block_num}
        return self._single_post(url, params)

    def get_from_public_key(self, public_key):
        """
        根据公钥获取账号信息
        :return:
        """
        url = full_url(self.url, "/account/get/from-public-key")
        params = {"publicKey": public_key}
        return self._single_get(url, params)


class NemParser:
    """xrp解析处理器"""
    def __init__(self):
        self.http = NemHttp(G_CFG)

    def newest_height(self):
        """
        获取最新区块高度
        :return:
        """
        result = self.http.get_block_count()
        return result["height"]

    def parse_block(self, block_num):
        """
        解析区块的交易详情
        :return: 返回待推送信息列表
        """
        block_num -= 1
        block = self.http.get_block(block_num)
        current_block = block["data"][0] if "data" in block.keys() and len(block["data"]) > 0 else None
        push_list = []
        if current_block:
            txs = current_block["txes"]
            block_num = current_block["block"]["height"]
            for tx in txs:
                tx_type = tx["tx"]["type"]
                if tx_type == 257:
                    mq_tx = G_PUSH_TEMPLATE.copy()
                    mq_tx["Txid"] = tx["hash"]
                    mq_tx["Type"] = "NEM"
                    # 根据公钥得到地址
                    signer = tx["tx"]["signer"]
                    result = self.http.get_from_public_key(signer)
                    mq_tx["From"] = result["account"]["address"]
                    mq_tx["To"] = tx["tx"]["recipient"]
                    mq_tx["Amount"] = tx["tx"]["amount"]
                    mq_tx["Time"] = tx["tx"]["timeStamp"]
                    mq_tx["BlockNumber"] = block_num
                    mq_tx["status"] = "true"
                    push_list.append(mq_tx)
        return push_list


class Nem(TxPlugin):

    name = "nem"
    desc = "解析nem数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = NemParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num):
        return self.parser.parse_block(block_num)
