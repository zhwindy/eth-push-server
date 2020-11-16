# -*- encode: utf-8 -*-
# Author: Arthur.Gao -- 黑小帅

from rpc.jsonrpc import HttpMixin
from core.globals import G_CFG, G_PUSH_TEMPLATE
from lib.tool import date_to_timestamp


class HtdfRpc(HttpMixin):

    def _get_url(self, short_url):
        return self.url.rstrip('/') + "/" + short_url.lstrip('/')

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
        url = self._get_url("/blocks/latest")
        return self._single_get(url)

    def get_block(self, block_num):
        """
        根据区块高度获取区块详情
        :param block_num: 区块高度
        :return:
        """
        url = self._get_url("block_detail/{}".format(block_num))
        return self._single_get(url)

    def get_transaction(self, tx_id):
        url = self._get_url("/transaction/{}".format(tx_id))
        return self._single_get(url)


class HtdfParser:
    """解析处理器"""

    def __init__(self, coin_name):
        self.coin_name = coin_name.upper()
        self.http = HtdfRpc(G_CFG)

    def newest_height(self):
        """
        获取最新区块高度
        :return:
        """
        result = self.http.get_block_count()
        return int(result['block_meta']['header']['height'])

    def parse_block(self, block_num):
        """
        解析区块的交易详情
        :return: 返回待推送信息列表
        """
        push_list = []
        block = self.http.get_block(block_num)
        block_time = block['block_meta']['header']['time']
        timestamp = str(date_to_timestamp(block_time.split(".")[0]))
        txs = block['block'].get("txs", [])
        if txs:
            for idx, tx in enumerate(txs):
                mq_tx = G_PUSH_TEMPLATE.copy()
                mq_tx['BlockNumber'] = block_num
                mq_tx['Time'] = timestamp
                mq_tx['From'] = tx['From']
                mq_tx['To'] = tx['To']
                mq_tx["Type"] = self.coin_name
                mq_tx["Txid"] = tx['Hash']
                mq_tx["TxIndex"] = idx
                mq_tx["status"] = "true"
                mq_tx["Memo"] = tx['Memo']

                tx_amount = tx['Amount']
                if tx_amount:
                    mq_tx['Amount'] = tx_amount[0]['amount']
                else:
                    continue
                push_list.append(mq_tx)
        return push_list
