#!/usr/bin/env python
from lib.iplugin import TxPlugin
from rpc.jsonrpc import JsonRpcRequest
from core.globals import G_CFG, G_PUSH_TEMPLATE

__all__ = ["Wicc"]


class WiccRpc(JsonRpcRequest):
    """wicc接口"""

    def __init__(self, cfg):
        super().__init__(cfg)

    def get_block_count(self):
        """
        获取节点当前最新高度，必须命名为newest_height
        """
        method = "getblockcount"
        params = []

        return self._single_post(method, params=params)

    def get_block(self, block_height):
        """
        获取区块详情
        :param block_height 十进制区块高度
        """
        assert isinstance(block_height, int)
        method = "getblock"
        params = [block_height]

        result = self._single_post(method, params=params)
        txs = result.get("tx", [])
        new_txs = []
        for tx_hash in txs:
            tx = self.get_transaction(tx_hash)
            new_txs.append(tx)
        result["tx"] = new_txs
        return result

    def get_transaction(self, tx_id):
        """
        获取单笔交易
        :param tx_id 交易ID
        """
        method = "gettxdetail"
        params = [tx_id]

        return self._single_post(method, params=params)


class WiccParser:
    """wicc解析处理器"""

    def __init__(self):
        self.rpc = WiccRpc(G_CFG)
        self.rollback = False
        self.rollback_count = 0

    def newest_height(self):
        """
        获取最新区块高度
        :return:
        """
        return self.rpc.get_block_count() - 6

    def parse_block(self, block_num):
        """
        解析区块的交易列表
        :return: 返回待推送信息列表
        """
        block = self.rpc.get_block(block_num)
        txs = block["tx"] if block else []
        push_list = []
        for tx in txs:
            tx_type = tx.get("tx_type")
            if tx_type in ["BCOIN_TRANSFER_TX", "UCOIN_TRANSFER_TX"]:
                transfers = tx.get("transfers", [])
                for i, tran in enumerate(transfers):
                    if tran.get("coin_symbol") != "WICC":
                        continue
                    mq_tx = G_PUSH_TEMPLATE.copy()
                    mq_tx["BlockNumber"] = tx["confirmed_height"]
                    mq_tx["Time"] = block["time"]
                    mq_tx["Txid"] = tx["txid"]
                    mq_tx["Type"] = "WICC"
                    mq_tx["From"] = tx["from_addr"]
                    mq_tx["To"] = tran["to_addr"]
                    mq_tx["Amount"] = tran["coin_amount"]
                    mq_tx["Fee"] = tx.get("fees", "")
                    mq_tx["Memo"] = tx.get("memo", "")
                    mq_tx["Action"] = tx_type
                    mq_tx["VoutsIndex"] = i
                    mq_tx["status"] = "true"
                    push_list.append(mq_tx)
            else:
                mq_tx = G_PUSH_TEMPLATE.copy()
                mq_tx["BlockNumber"] = tx["confirmed_height"]
                mq_tx["Time"] = block["time"]
                mq_tx["Txid"] = tx["txid"]
                mq_tx["Type"] = "WICC"
                mq_tx["From"] = tx.get("from_addr", "")
                mq_tx["To"] = tx.get("to_addr", "")
                mq_tx["Amount"] = tx.get("coin_amount", "")
                mq_tx["Fee"] = tx.get("fees", "")
                mq_tx["Memo"] = tx.get("memo", "")
                mq_tx["Action"] = tx_type
                mq_tx["status"] = "true"
                push_list.append(mq_tx)
        return push_list


class Wicc(TxPlugin):

    name = "wicc"
    desc = "解析wicc数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = WiccParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        return self.parser.parse_block(block_num)


if __name__ == "__main__":
    import json
    e = Wicc()
    b = 70000
    r = e.push_list(b)
    print(json.dumps(r))
