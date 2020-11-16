#!/usr/bin/env python
from lib.error import UrlError
from lib.iplugin import TxPlugin
from rpc.httplib import HttpMixin
from decimal import Decimal
from core.globals import G_CFG, G_PUSH_TEMPLATE, G_LOGGER
from lib.tool import full_url, date_to_timestamp
import random
import time

__all__ = ["Eos"]


class EosHttp(HttpMixin):
    """eos http接口"""
    def __init__(self, cfg):
        timeout = 60
        super().__init__(timeout)
        self.url = cfg.rpc.rpc_dict["url"]
        self.urls = ["eos.greymass.com"] * 1 + ["api.eossweden.org"] * 2
        self.set_json()

    def get_block_count(self):
        """
        获取最新高度
        :return:
        """
        url = full_url(self.url, "/v1/chain/get_info")
        return self._single_get(url)

    def get_block(self, block_num):
        """
        根据区块高度获取区块详情
        :param block_num: 区块高度
        :return:
        """
        url = full_url(self.url, "/v1/chain/get_block")
        # node = random.choice(self.urls)
        # node = "eos.greymass.com"
        # url = "http://{}/v1/chain/get_block".format(node)
        params = {"block_num_or_id": block_num}
        return url, self._single_post(url, params)

    def get_transaction(self, tx_id):
        url = full_url(self.url, "/v1/history/get_transaction")
        params = {"id": tx_id}
        return self._single_post(url, params)


class EosParser:
    """Eos解析处理器"""

    action_convert = {"transfer": {"from": "from", "to": "to", "amount": "quantity"},
                      "buyram": {"from": "payer", "to": "receiver", "amount": "quant"},
                      "buyrambytes": {"from": "payer", "to": "receiver", "amount": "bytes"},
                      "sellram": {"from": "account", "to": "account", "amount": "bytes"},
                      "delegatebw": {"from": "from", "to": "receiver", "cpu_amount": "stake_cpu_quantity", "net_amount": "stake_net_quantity"},
                      "undelegatebw": {"from": "from", "to": "receiver", "cpu_amount": "unstake_cpu_quantity", "net_amount": "unstake_net_quantity"},
                      "other_delegatebw": {"cpu_amount": "cpu_quantity", "net_amount": "net_quantity"},
                      "other_undelegatebw": {"cpu_amount": "cpu_quantity", "net_amount": "net_quantity"},
                      }
    action_account = {"transfer": "eosio.token",
                      "buyram": "eosio",
                      "buyrambytes": "eosio",
                      "sellram": "eosio",
                      "delegatebw": "eosio",
                      "undelegatebw": "eosio"
                      }
    SUPPORTED = ["transfer", "buyram", "buyrambytes", "sellram", "delegatebw", "undelegatebw"]

    def __init__(self):
        self.http = EosHttp(G_CFG)

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
        t1 = time.time()
        push_list = []
        node, block = self.http.get_block(block_num)
        t2 = time.time()
        if isinstance(block, str):
            # 出现大块，比如110588067，api.eossweden.org超级节点偶发性返回部分数据，不能被json反序列化，需要多次重试才能获得结果，造成消息积压
            # eos.greymass.com超级节点可以正确拿到数据，但是访问频率受限，所以异常以后要重试获取数据
            G_LOGGER.info("eos_pass, block_num={}, node={}, lenght={}, head={}, tail={}".format(block_num, node, len(block), block[:38], block[-30:]))
            # self.redis.client.rpush("eos_pass_block", block_num)
            # return push_list
        timestamp = str(date_to_timestamp(block['timestamp'].split(".")[0]))
        txs = block.get("transactions", [])
        for tx in txs:
            tx_status = tx.get("status")
            if (not tx_status) or (tx_status not in ("executed")):
                continue
            trx = tx.get("trx", "")
            if not isinstance(trx, dict):
                continue
            tx_id = trx['id']
            transaction = trx["transaction"]
            acts = transaction.get('actions', [])
            act_index = 0
            get_amount_and_symbol = self.get_amount_and_symbol
            for act in acts:
                account, name, Contract = act.get('account'), act['name'], ""
                # if name in self.SUPPORTED and self.action_account.get(name) == account:
                if name in self.SUPPORTED:
                    data = act['data']
                    if not isinstance(data, dict):
                        continue
                    mq_tx = G_PUSH_TEMPLATE.copy()
                    if name == "delegatebw" and self.action_account.get(name) != account:
                        from_addr = account
                        to_addr = data.get("to")
                        amount, symbol = get_amount_and_symbol("other_delegatebw", data)
                    elif name == "undelegatebw" and self.action_account.get(name) != account:
                        from_addr = data.get("from")
                        to_addr = account
                        amount, symbol = get_amount_and_symbol("other_undelegatebw", data)
                    else:
                        from_addr = data.get(self.action_convert[name]['from'])
                        to_addr = data.get(self.action_convert[name]['to'])
                        if not from_addr or not to_addr:
                            continue
                        amount, symbol = get_amount_and_symbol(name, data)
                    # 无金额或者金额小于0.0001, 略过
                    if (not amount) or (float(amount) <= 0.0001):
                        continue
                    if account and str(account) == "eosio.token" and symbol in ["EOS"]:
                        Contract = "eosio.token"
                    else:
                        Contract = str(account) + "|" + str(symbol).upper()
                    memo_info = data.get("memo", "")
                    mq_tx["BlockNumber"] = block_num
                    mq_tx["Time"] = timestamp
                    mq_tx["Txid"] = tx_id
                    mq_tx["Type"] = "EOS"
                    mq_tx['From'] = from_addr
                    mq_tx['To'] = to_addr
                    mq_tx['Amount'] = amount
                    mq_tx["Action"] = name
                    mq_tx['Contract'] = Contract
                    mq_tx["VoutsIndex"] = act_index
                    mq_tx["Memo"] = memo_info
                    mq_tx["Valid"] = True
                    mq_tx["status"] = "true"
                    push_list.append(mq_tx)
                    act_index += 1
        t3 = time.time()
        http_time = int((t2 - t1) * 1000) / 1000
        parse_time = int((t3 - t2) * 1000) / 1000
        G_LOGGER.info("eos_parse_block, block_num={}, length={}, node={}, 网络耗时={}, 处理耗时={}".format(block_num, len(push_list), node, http_time, parse_time))
        return push_list

    def get_amount_and_symbol(self, action_name, data):
        """
        解析金额和token名
        """
        amount = 0
        symbol = "EOS"
        if action_name in ["transfer", "buyram"]:
            amount_str = data.get(self.action_convert[action_name]['amount'])
            if not amount_str:
                return amount, symbol
            amount, symbol = self.parse_amount_and_symbol(amount_str)
        elif action_name in ["buyrambytes", "sellram"]:
            amount = data.get(self.action_convert[action_name]['amount'])
            if not amount:
                return amount, symbol
        else:
            cpu_amount_str = data.get(self.action_convert[action_name]['cpu_amount'])
            if not cpu_amount_str:
                if action_name == "other_delegatebw":
                    cpu_amount_str = data.get(self.action_convert['delegatebw']['cpu_amount'])
                elif action_name == "other_undelegatebw":
                    cpu_amount_str = data.get(self.action_convert['undelegatebw']['cpu_amount'])
                else:
                    return amount, symbol
            cpu_amount, _ = self.parse_amount_and_symbol(cpu_amount_str)
            net_amount_str = data.get(self.action_convert[action_name]['net_amount'])
            if not net_amount_str:
                if action_name == "other_delegatebw":
                    net_amount_str = data.get(self.action_convert['delegatebw']['net_amount'])
                elif action_name == "other_undelegatebw":
                    net_amount_str = data.get(self.action_convert['undelegatebw']['net_amount'])
                else:
                    return amount, symbol
            net_amount, _ = self.parse_amount_and_symbol(net_amount_str)
            total_amount = Decimal(str(cpu_amount)) + Decimal(str(net_amount))
            amount = str(total_amount)

        return amount, symbol

    def parse_amount_and_symbol(self, amount_str):
        """
        解析金额和token名
        """
        amount = 0
        symbol = "EOS"
        value = amount_str.split(" ") if amount_str else 0
        if value and len(value) > 1:
            amount = value[0]
            symbol = str(value[1]).upper()
        return amount, symbol


class Eos(TxPlugin):

    name = "eos"
    desc = "解析eos数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = EosParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        return self.parser.parse_block(block_num)


if __name__ == '__main__':
    eos = Eos()
    r = eos.push_list(53981890)
    print(r)
