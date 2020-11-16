#!/usr/bin/env python
import time
from lib.iplugin import TxPlugin
from rpc.jsonrpc import JsonRpcRequest
from core.globals import G_CFG, G_PUSH_TEMPLATE, G_LOGGER

__all__ = ["Ont"]


class OntRpc(JsonRpcRequest):
    """ont rpc接口"""
    def __init__(self, cfg):
        super().__init__(cfg)

    def get_block_count(self):
        """
        获取最新高度
        :return:
        """
        method = "getblockcount"
        return self._single_post(method)

    def get_block(self, block_num):
        """
        根据区块高度获取区块详情
        :param block_num: 区块高度
        :return:
        """
        method = "getblock"
        params = [block_num, 1]
        return self._single_post(method, params)

    def get_smart_code_event(self, block_num):
        """
        根据区块高度获取区块里面合约执行结果
        :param block_num:
        :return:
        """
        method = "getsmartcodeevent"
        params = [block_num]
        return self._single_post(method, params)


class OntParser:
    """ont解析处理器"""
    def __init__(self):
        self.rpc = OntRpc(G_CFG)

    def newest_height(self):
        """
        获取最新区块高度
        :return:
        """
        return self.rpc.get_block_count()

    def parse_block(self, block_num):
        """
        解析区块的交易详情
        :return: 返回待推送信息列表
        """
        push_list = []
        block = self.rpc.get_block(block_num)
        timestamp = block["Header"]["Timestamp"]
        txs = self.rpc.get_smart_code_event(block_num)
        for tx in txs:
            # 不成功的不推送
            if tx["State"] != 1:
                continue

            for n in tx["Notify"]:
                # 只支持ont和ong的推送, 且只推送类型为transfer的交易
                if n["ContractAddress"] not in ["0100000000000000000000000000000000000000",
                                                "0200000000000000000000000000000000000000"] or \
                        n["States"][0] != "transfer":
                    continue
                mq_tx = G_PUSH_TEMPLATE.copy()
                mq_tx["BlockNumber"] = block_num
                mq_tx["Time"] = timestamp
                mq_tx["Txid"] = tx["TxHash"]
                mq_tx["Type"] = "ONT"
                mq_tx["From"] = n["States"][1]
                mq_tx["To"] = n["States"][2]
                mq_tx["Amount"] = n["States"][3]
                mq_tx["Contract"] = n["ContractAddress"]
                mq_tx["status"] = "true"
                push_list.append(mq_tx)
        return push_list


class Ont(TxPlugin):

    name = "ont"
    desc = "解析ont数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = OntParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num):
        return self.parser.parse_block(block_num)


if __name__ == "__main__":
    t = Ont()
    import json
    d = t.push_list(2030554)
    print(json.dumps(d))
