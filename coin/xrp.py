#!/usr/bin/env python
import time
from lib.iplugin import TxPlugin
from rpc.jsonrpc import JsonRpcRequest
from core.globals import G_CFG, G_PUSH_TEMPLATE, G_LOGGER

__all__ = ["Xrp"]


class XrpRpc(JsonRpcRequest):
    """xrp rpc接口"""
    def __init__(self, cfg):
        super().__init__(cfg)
        self.timeout = 30

    def get_block_count(self):
        """
        获取最新高度
        :return:
        """
        method = "ledger_current"
        return self._single_post(method)

    def get_block(self, block_num):
        """
        根据区块高度获取区块详情
        :param block_num: 区块高度
        :return:
        """
        if isinstance(block_num, str):
            block_num = int(block_num)
        assert isinstance(block_num, int)
        method = "ledger"
        params = [
            {
                "ledger_index": block_num,
                "transactions": True,
                "expand": True
            }
        ]
        return self._single_post(method, params)


class XrpParser:
    """xrp解析处理器"""
    def __init__(self):
        self.rpc = XrpRpc(G_CFG)
        self.time_diff = 946684800

    def newest_height(self):
        """
        获取最新区块高度
        :return:
        """
        result = self.rpc.get_block_count()
        height = result["ledger_current_index"]
        return height - 31 if height else None

    def parse_block(self, block_num):
        """
        解析区块的交易详情
        :return: 返回待推送信息列表
        """
        block = None
        while True:
            try:
                block = self.rpc.get_block(block_num)
                ledger = block.get("ledger", []) if block else []
                txs = ledger.get("transactions", []) if ledger else []
                push_list = []
                for tx in txs:
                    if tx["TransactionType"] == "Payment" and isinstance(tx['Amount'], str):
                        # 交易类型为Payment且状态为成功
                        mq_tx = G_PUSH_TEMPLATE.copy()
                        mq_tx["Txid"] = tx.get("hash", "")
                        mq_tx["Type"] = "XRP"
                        mq_tx["From"] = tx.get("Account", "")
                        mq_tx["To"] = tx.get("Destination", "")
                        mq_tx["Fee"] = tx.get("Fee", "")
                        mq_tx["Memo"] = tx.get("DestinationTag", "")
                        meta_data = tx.get("metaData", {})
                        status = meta_data.get("TransactionResult")
                        mq_tx["Valid"] = True if (status and status == "tesSUCCESS") else False
                        mq_tx["status"] = "true" if (status and status == "tesSUCCESS") else "false"
                        mq_tx["Amount"] = meta_data.get("delivered_amount", "0")
                        mq_tx["Time"] = ledger.get("close_time", 0) + self.time_diff
                        mq_tx["BlockNumber"] = ledger.get("ledger_index", 0)
                        push_list.append(mq_tx)
                return push_list
            except Exception as e:
                G_LOGGER.info(f"区块{block_num}出现异常，区块内容:{block}，尝试重新获取。异常原因：{str(e)}")
                time.sleep(1)


class Xrp(TxPlugin):

    name = "xrp"
    desc = "解析xrp数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = XrpParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        return self.parser.parse_block(block_num)


if __name__ == "__main__":
    import json
    e = Xrp()
    b = 50292766
    r = e.push_list(b)
    print(json.dumps(r))
