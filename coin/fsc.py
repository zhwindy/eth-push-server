#!/usr/bin/env python

from lib.error import UrlError
from lib.iplugin import TxPlugin
from rpc.httplib import HttpMixin
from core.globals import G_CFG, G_PUSH_TEMPLATE
from lib.tool import full_url, date_to_timestamp

__all__ = ["Fsc"]


class FscHttp(HttpMixin):
    """
    fsc http接口
    """
    def __init__(self, cfg):
        timeout = cfg.rpc.rpc_dict.get("timeout")
        super().__init__(timeout)
        self.url = cfg.rpc.rpc_dict["url"]
        self.set_json()

    def get_block_count(self):
        """
        获取最新高度
        """
        url = full_url(self.url, "/v1/chain/get_info")
        return self._single_get(url)

    def get_block(self, block_num):
        """
        根据区块高度获取区块详情
        """
        url = full_url(self.url, "/v1/chain/get_block")
        params = {"block_num_or_id": block_num}
        return self._single_post(url, params)

    def get_transaction(self, tx_id):
        url = full_url(self.url, "/v1/history/get_transaction")
        params = {"id": tx_id}
        return self._single_post(url, params)


class FscParser:
    """
    fsc解析处理器
    """

    action_convert = {"transfer": {"from": "from", "to": "to", "amount": "quantity"},
                      "buyram": {"from": "payer", "to": "receiver", "amount": "quant"},
                      "buyrambytes": {"from": "payer", "to": "receiver", "amount": "bytes"},
                      "sellram": {"from": "account", "to": "account", "amount": "bytes"},
                      "delegatebw": {"from": "payer", "to": "receiver", "amount": "stake_cpu_quantity"},
                      "undelegatebw": {"from": "payer", "to": "receiver", "amount": "unstake_cpu_quantity"},
                      }
    action_account = {"transfer": "fscio.token",
                      "buyram": "fscio",
                      "buyrambytes": "fscio",
                      "sellram": "fscio",
                      "delegatebw": "fscio",
                      "undelegatebw": "fscio"
                      }
    SUPPORTED = ['transfer']

    def __init__(self):
        self.http = FscHttp(G_CFG)

    def newest_height(self):
        """
        获取最新区块高度
        :return:
        """
        result = self.http.get_block_count()
        return result["head_block_num"]

    def parse_block(self, block_num):
        """
        解析区块的交易详情
        :return: 返回待推送信息列表
        """
        push_list = []
        block = self.http.get_block(block_num)
        timestamp = str(date_to_timestamp(block['timestamp'].split(".")[0]))
        txs = block.get("transactions", [])
        for tx in txs:
            trx = tx.get("trx", "")
            if not isinstance(trx, dict):
                continue
            tx_id = trx['id']
            transaction = trx["transaction"]
            acts = transaction.get('actions', [])
            act_index = 0
            for act in acts:
                account, name = act['account'], act['name']
                if name in self.SUPPORTED and self.action_account.get(name) == account:
                    data = act['data']
                    if not isinstance(data, dict):
                        continue
                    mq_tx = G_PUSH_TEMPLATE.copy()
                    from_addr = data[self.action_convert[name]['from']]
                    to_addr = data[self.action_convert[name]['to']]
                    amount = data[self.action_convert[name]['amount']]
                    mq_tx["BlockNumber"] = block_num
                    mq_tx["Time"] = timestamp
                    mq_tx["Txid"] = tx_id
                    mq_tx["Type"] = "FSC"
                    mq_tx['From'] = from_addr
                    mq_tx['To'] = to_addr
                    mq_tx['Amount'] = amount
                    mq_tx['Contract'] = account
                    mq_tx["TxIndex"] = act_index
                    tx_detail = dict()
                    try:
                        tx_detail = self.http.get_transaction(tx_id)
                    except UrlError as e:
                        if str(e).find("URL不存在") > -1:
                            continue
                    traces = tx_detail.get("traces", [])
                    if not traces:
                        traces = tx_detail.get("action_traces", [])
                    for trace in traces:
                        data = trace["act"]["data"]
                        if (isinstance(data, dict) and data.get("from") == from_addr and data.get("to") == to_addr and data.get("quantity") == amount):
                            mq_tx["ActDigest"] = trace["receipt"]["act_digest"]
                    push_list.append(mq_tx)
                    act_index += 1
        return push_list


class Fsc(TxPlugin):

    name = "fsc"
    desc = "解析fsc数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = FscParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num):
        return self.parser.parse_block(block_num)


if __name__ == '__main__':
    fsc = Fsc()
    r = fsc.push_list(6125810)
    print(r)
