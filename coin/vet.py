#!/usr/bin/env python
from lib.tool import full_url
from lib.iplugin import TxPlugin
from rpc.jsonrpc import HttpMixin
from core.globals import G_CFG, G_PUSH_TEMPLATE

__all__ = ["Vet"]


class VetRpc(HttpMixin):
    """xlm http接口"""
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
        url = full_url(self.url, "blocks/best")
        ledger_detail = self._single_get(url)
        return ledger_detail

    def get_block_transactions(self, block_num):
        """
        根据区块高度获取区块交易id
        """
        url = full_url(self.url, "blocks/{}".format(block_num))
        ledger_details = self._single_get(url)
        return ledger_details.get('transactions', [])

    def get_transactions(self, transaction):
        """
        根据tx_hash获取具体交易记录
        """
        url = full_url(self.url, "transactions/{}".format(transaction))
        ledger_details = self._single_get(url)
        return ledger_details


class XlmParser:
    """xrp解析处理器"""
    def __init__(self):
        self.rpc = VetRpc(G_CFG)

    def newest_height(self):
        """
        获取最新区块高度
        :return:
        """
        result = self.rpc.get_block_count()
        return result.get('number', 0)

    def parse_block(self, block_number):
        """
        解析区块的交易列表
        :return: 返回待推送信息列表
        """
        txs = self.rpc.get_block_transactions(block_number)
        push_list = []
        for tx in txs:
            mq_tx = G_PUSH_TEMPLATE.copy()
            tx_details = self.rpc.get_transactions(tx)
            mq_tx["BlockNumber"] = block_number
            mq_tx["Time"] = tx_details.get('meta', {}).get('blockTimestamp')
            mq_tx["Txid"] = tx
            mq_tx["Type"] = "VET"
            mq_tx['From'] = tx_details.get('origin')
            mq_tx['To'] = tx_details.get('clauses', [])[0].get('to')
            mq_tx['Amount'] = int(tx_details.get('clauses', [])[0].get('value'), 16)
            push_list.append(mq_tx)


class Vet(TxPlugin):

    name = "vet"
    desc = "解析xlm数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = XlmParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num):
        return self.parser.parse_block(block_num)


if __name__ == "__main__":
    t = Vet()
    import json
    d = t.push_list(22164357)
    print(json.dumps(d))
