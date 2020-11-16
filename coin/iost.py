#!/usr/bin/env python
# @Time    : 2019-8-20 9:36
# @Author  : Humingxing
# @File    : iost.py
# @DESC    : iost交易数据解析插件

from json import loads
from lib.tool import full_url
from lib.iplugin import TxPlugin
from rpc.jsonrpc import HttpMixin
from core.globals import G_CFG, G_PUSH_TEMPLATE

__all__ = ["Iost"]


class IostRpc(HttpMixin):
    """iost http接口"""
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
        url = full_url(self.url, "getChainInfo")
        result = self._single_get(url)
        return int(result["lib_block"])

    def get_block(self, block_num):
        """
        根据区块高度获取区块详情
        :param block_num: 区块高度
        :return:
        """
        url = full_url(self.url, f"/getBlockByNumber/{block_num}/true")
        return self._single_get(url)


class IostParser:
    """iost解析处理器"""

    def __init__(self):
        self.rpc = IostRpc(G_CFG)
        self.action_account = {
            "transfer": "token.iost",
            "buy": "ram.iost",
            "sell": "ram.iost",
            "pledge": "gas.iost",
            "unpledge": "gas.iost"
        }
        self.supported = ["transfer", "buy", "sell", "pledge", "unpledge"]

        self.action_map = {
            "transfer": "transfer",
            "buy": "buyrambytes",
            "sell": "sellram",
            "pledge": "delegatebw",
            "unpledge": "undelegatebw",
        }

    def newest_height(self):
        """
        获取最新区块高度
        :return:
        """
        return self.rpc.get_block_count() - 10

    def parse_block(self, block_num):
        """
        解析区块的交易列表
        :return: 返回待推送信息列表
        """
        block = self.rpc.get_block(block_num)
        block = block.get("block", {})
        txs = block.get("transactions", []) if block else []

        push_list = []
        for tx in txs:
            tx_receipt = tx.get("tx_receipt", {})
            actions = tx.get("actions", [])
            for i, act in enumerate(actions):
                contract, action_name, data = act.get("contract", ""), act.get("action_name", ""), act.get("data", "")
                if action_name in self.supported:
                    data = loads(data)
                    d_len = len(data)
                    _from, _to, _amount, _memo = "", "", 0, ""
                    if action_name == "transfer" and contract == "token.iost":
                        _token = data[0] if d_len > 0 else ""
                        if _token and _token == "iost":
                            contract = "iost"
                        else:
                            contract = _token
                        _from = data[1] if d_len > 1 else ""
                        _to = data[2] if d_len > 2 else ""
                        _amount = data[3] if d_len > 3 else ""
                        _memo = data[4] if d_len > 4 else ""
                    elif action_name in ["buy", "sell", "pledge", "unpledge"]:
                        if d_len < 4:
                            continue
                        contract = "iost"
                        _from = data[0] if d_len > 0 else ""
                        _to = data[1] if d_len > 1 else ""
                        _amount = data[2] if d_len > 2 else ""
                    else:
                        continue
                    mq_tx = G_PUSH_TEMPLATE.copy()
                    mq_tx["BlockNumber"] = block.get("number", "-1")
                    mq_tx["Time"] = block.get("time", "0")[:10]
                    mq_tx["Txid"] = tx.get("hash", "")
                    mq_tx["Type"] = "IOST"
                    mq_tx["From"] = _from
                    mq_tx["To"] = _to
                    mq_tx["Amount"] = _amount
                    mq_tx["Contract"] = contract
                    mq_tx["Memo"] = _memo
                    mq_tx["Fee"] = 0
                    mq_tx["Action"] = self.action_map.get(action_name, "")
                    mq_tx["VoutsIndex"] = i
                    status = tx_receipt.get("status_code", True)
                    mq_tx["Valid"] = True if status == "SUCCESS" else False
                    mq_tx["status"] = "true" if status == "SUCCESS" else "false"
                    push_list.append(mq_tx)
        return push_list


class Iost(TxPlugin):

    name = "iost"
    desc = "解析iost数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = IostParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        return self.parser.parse_block(block_num)


if __name__ == "__main__":
    import json
    import time
    e = Iost()
    b = 54570470
    r = e.push_list(b)
    print(json.dumps(r))
