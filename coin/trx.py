#!/usr/bin/env python
# -*- encode: utf-8 -*-
from json import loads
from base58 import b58encode_check
from lib.tool import full_url
from lib.iplugin import TxPlugin
from rpc.jsonrpc import HttpMixin
from core.globals import G_CFG, G_PUSH_TEMPLATE

__all__ = ["Trx"]


class TrxRpc(HttpMixin):
    """trx http接口"""
    def __init__(self, cfg):
        self.cache = {}
        timeout = cfg.rpc.rpc_dict.get("timeout")
        super().__init__(timeout)
        self.url = cfg.rpc.rpc_dict["url"]
        self.set_json()

    def get_block_count(self):
        """
        获取最新高度
        :return:
        """
        url = full_url(self.url, "/walletsolidity/getnowblock")
        result = self._single_post(url)
        block_height = result.get("block_header", {}).get("raw_data", {}).get("number")
        return int(block_height)

    def get_block(self, block_num):
        """
        根据区块高度获取区块详情
        :param block_num: 区块高度
        :return:
        """
        url = full_url(self.url, "/walletsolidity/getblockbynum")
        params = {"num": int(block_num)}
        return self._single_post(url, params)

    def trc10_token_name(self, token_id):
        if token_id not in self.cache.keys():
            url = f"https://apilist.tronscan.org/api/token?showAll=1&limit=5000&id={token_id}&fields=id,abbr"
            self.cache[token_id] = self._single_get(url)
        return self.cache[token_id]


class TrxParser:
    """trx解析处理器"""

    def __init__(self):
        self.rpc = TrxRpc(G_CFG)
        self.supported = ["TransferContract", "TransferAssetContract", "TriggerSmartContract",
                          "FreezeBalanceContract", "UnfreezeBalanceContract"]
        self.contract = {"TransferContract": "trx", "TransferAssetContract": "trc10", "TriggerSmartContract": "trc20"}
        self.action_map = {
            "TransferContract": "transfer",
            "FreezeBalanceContract": "delegatebw",
            "UnfreezeBalanceContract": "undelegatebw",
            "TransferAssetContract": "transfer",
            "TriggerSmartContract": "transfer",
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
        blocknumber = block.get("block_header", {}).get("raw_data", {}).get("number", -1)
        block_timestamp = block.get("block_header", {}).get("raw_data", {}).get("timestamp", "")
        txs = block.get("transactions", []) if block else []
        push_list = []
        for tx in txs:
            # ret = tx.get("ret", [])
            status = tx.get("ret", [])[0].get("contractRet", "")
            if status != "SUCCESS":
                continue
            txid = tx.get("txID", "")
            raw_data = tx.get("raw_data", {})
            # timestamp_update = raw_data.get("timestamp", "")
            timestamp = block_timestamp
            contract = raw_data.get("contract", [])
            if not contract:
                continue
            else:
                contract = contract[0]
            transaction_type = contract.get("type", "")
            if transaction_type not in self.supported:
                continue
            transaction_data = contract.get("parameter", {}).get("value", {})
            _from = b58encode_check(bytes.fromhex(transaction_data.get("owner_address", ""))).decode()
            # contract_type = self.contract.get(transaction_type)
            if transaction_type == "TransferContract":
                _to = b58encode_check(bytes.fromhex(transaction_data.get("to_address", ""))).decode()
                _amount = transaction_data.get("amount", 0)
                contract_type = 0
            elif transaction_type == "FreezeBalanceContract":
                receiver_address = transaction_data.get("receiver_address", "")
                _to = b58encode_check(bytes.fromhex(receiver_address)).decode() if receiver_address else _from
                _amount = transaction_data.get("frozen_balance", 0)
                contract_type = ""
            elif transaction_type == "UnfreezeBalanceContract":
                receiver_address = transaction_data.get("receiver_address", "")
                _to = b58encode_check(bytes.fromhex(receiver_address)).decode() if receiver_address else _from
                _amount = transaction_data.get("frozen_balance", 0)
                contract_type = ""
            elif transaction_type == "TransferAssetContract":
                _from = b58encode_check(bytes.fromhex(transaction_data.get("owner_address"))).decode()
                _to = b58encode_check(bytes.fromhex(transaction_data.get("to_address"))).decode()
                asset_name = transaction_data.get("asset_name")
                contract_type = bytes.fromhex(asset_name).decode("utf-8")
                _amount = transaction_data.get("amount", 0)
            else:
                data = transaction_data.get("data")
                if data and data[:8] == "a9059cbb":
                    _amount = int(data[72:], 16)
                    _to = b58encode_check(bytes.fromhex(f"41{data[32:72]}")).decode()
                    # after b58encode_check the legal address start with 0x41(mainnet) or 0xa0(testnet)
                    contract_address = contract.get("parameter", {}).get("value", {}).get("contract_address")
                    contract_type = b58encode_check(bytes.fromhex(contract_address)).decode()
                else:
                    continue
            mq_tx = G_PUSH_TEMPLATE.copy()
            mq_tx["BlockNumber"] = blocknumber
            mq_tx["Time"] = int(timestamp)//1000
            mq_tx["Txid"] = txid
            mq_tx["Type"] = "TRX"
            mq_tx["From"] = _from
            mq_tx["To"] = _to
            mq_tx["Amount"] = int(_amount)
            mq_tx["Contract"] = str(contract_type)
            mq_tx["Fee"] = 0
            mq_tx["Action"] = self.action_map[transaction_type]
            mq_tx["Valid"] = True if status == "SUCCESS" else False
            mq_tx["status"] = "true" if status == "SUCCESS" else "false"
            push_list.append(mq_tx)
        return push_list


class Trx(TxPlugin):

    name = "trx"
    desc = "解析trx数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = TrxParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        return self.parser.parse_block(block_num)


if __name__ == "__main__":
    import json
    import time
    e = Trx()
    # b = 14490544
    # b = 14604225
    b = 14604226
    r = e.push_list(b)
    print(json.dumps(r))
