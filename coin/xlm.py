#!/usr/bin/env python
from lib.tool import full_url
from lib.iplugin import TxPlugin
from rpc.jsonrpc import HttpMixin
from core.globals import G_CFG, G_PUSH_TEMPLATE
from lib.tool import date_z_to_timestamp

__all__ = ["Xlm"]


class XlmRpc(HttpMixin):
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
        url = full_url(self.url, "ledgers")
        params = {'limit': 1, 'order': 'desc'}
        ledger_detail = self._single_get(url, params=params)
        return ledger_detail

    def get_block(self, block_num):
        """
        根据区块高度获取区块详情
        :param block_num: 区块高度
        :return:
        """
        url = full_url(self.url, "ledgers/{}/operations".format(block_num))
        ledger_details = self._single_get(url)
        ledger_detail = ledger_details.get('_embedded', {}).get('records', [])
        return ledger_detail


class XlmParser:
    """xrp解析处理器"""
    def __init__(self):
        self.rpc = XlmRpc(G_CFG)

    def newest_height(self):
        """
        获取最新区块高度
        :return:
        """
        result = self.rpc.get_block_count()
        return result.get('_embedded', {}).get('records', [])[0].get('sequence')

    def parse_block(self, block_num):
        """
        解析区块的交易列表
        :return: 返回待推送信息列表
        """
        txs = self.rpc.get_block(block_num)
        push_list = []
        for tx in txs:
            mq_tx = G_PUSH_TEMPLATE.copy()
            tx_type = tx.get('type', '')
            if tx_type not in ('payment', 'create_account'):
                continue
            mq_tx["BlockNumber"] = block_num
            mq_tx["Time"] = date_z_to_timestamp(tx.get('created_at'))
            mq_tx["Txid"] = tx.get('transaction_hash')
            mq_tx["Type"] = "XLM"
            mq_tx["status"] = "true"
            if tx_type == 'payment':
                mq_tx['From'] = tx.get('from')
                mq_tx['To'] = tx.get('to')
                mq_tx['Amount'] = tx.get('amount')
            elif tx_type == 'create_account':
                mq_tx['From'] = tx.get('funder')
                mq_tx['To'] = tx.get('account')
                mq_tx['Amount'] = tx.get('starting_balance')
            push_list.append(mq_tx)
        return push_list


class Xlm(TxPlugin):

    name = "xlm"
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
    t = Xlm()
    import json
    d = t.newest_height()
    print(json.dumps(d))
