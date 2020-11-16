#!/usr/bin/env python
# @Time    : 18-10-10 上午10:40
# @Author  : Humingxing
# @File    : act.py
# @DESC    : act交易数据解析插件
from lib.iplugin import TxPlugin
from rpc.jsonrpc import JsonRpcRequest
from core.globals import G_CFG, G_PUSH_TEMPLATE, G_LOGGER
from lib.tool import date_to_timestamp

__all__ = ["Act"]


class ActRpc(JsonRpcRequest):
    """act rpc接口"""
    def __init__(self, cfg):
        super().__init__(cfg)

    def get_block_count(self):
        """
        获取最新高度
        :return:
        """
        params = [1, ]
        method = "blockchain_get_block_count"
        return self._single_post(method, params)

    def get_block(self, block_num):
        """
        根据区块高度获取区块详情
        :param block_num: 区块高度
        :return:
        """
        method = "blockchain_get_block"
        params = [block_num, 1]
        return self._single_post(method, params)

    def get_transaction(self, tx_id):
        """
        根据交易ID获取交易详情（概要）
        :param tx_id: 交易ID
        :return:
        """
        method = "blockchain_get_transaction"
        params = [tx_id, ]
        return self._single_post(method, params)

    def get_pretty_contract_transaction(self, tx_id):
        """
        根据交易ID获取合约交易详情
        :param tx_id: 交易ID
        :return:
        """
        method = "blockchain_get_pretty_contract_transaction"
        params = [tx_id, ]
        return self._single_post(method, params)

    def get_pretty_transaction(self, tx_id):
        """
        根据交易ID获取普通交易（act交易）详情
        :param tx_id: 交易ID
        :return:
        """
        method = "blockchain_get_pretty_transaction"
        params = [tx_id, ]
        return self._single_post(method, params)


class ActParser:
    """act解析处理器"""
    def __init__(self):
        self.rpc = ActRpc(G_CFG)
        self.rollback = False
        self.rollback_count = 0

    def newest_height(self):
        """
        获取最新区块高度
        :return:
        """
        return self.rpc.get_block_count()

    def parse_block(self, block_num):
        """
        解析区块
        :return: 返回待推送信息列表
        """
        block = self.rpc.get_block(block_num)
        # 根据块中的交易拿到交易ID
        tx_ids = block["user_transaction_ids"]
        return self.parse_tx(tx_ids)

    def parse_tx(self, tx_ids):
        """
        解析交易详情
        :param tx_ids: 交易ids
        :return:
        """
        push_list = []
        for tx_id in tx_ids:
            # 根据tx_id查询交易详情
            tx = self.rpc.get_transaction(tx_id)
            # 获取operations下的第一个type字段（交易类型）
            tx_type = tx[1]["trx"]["operations"][0]["type"]
            asset_id = tx[1]["trx"]["alp_inport_asset"]["asset_id"]
            if tx_type == "transaction_op_type":
                # 合约交易
                tx = self.rpc.get_pretty_contract_transaction(tx_id)
            elif asset_id == 0:
                """
                已知tx_type
                withdraw_op_type: ACT Transfer (1f279cf35e8ec5cc785642bf06c57b1ba33a645b)
                deposit_op_type: ACT Transfer
                define_slate_op_type: ACT Transfer (09749dbf3aba3a54a5a5ac6f1723869becaf2b32)
                withdraw_pay_op_type: Agent Gets Paid (d81726e747a4e7c14ef12839cfb0dbd68117757f)
                register_account_op_type: Account Registration (f2de809451b084e8d80758efa7949d7ec8e80036)
                """
                # ACT交易
                tx = self.rpc.get_pretty_transaction(tx_id)
            else:
                G_LOGGER.info(f"unkown asset_id:{str(id)}, tx_type:{tx_type}, hash:{tx_id}")
                continue
            tx["tx_type"] = tx_type

            mq_tx = G_PUSH_TEMPLATE.copy()
            mq_tx["Type"] = "ACT"
            mq_tx["BlockNumber"] = tx["block_num"]
            mq_tx["Time"] = date_to_timestamp(tx["timestamp"])
            mq_tx["TxIndex"] = 0
            # 只要转账的合约交易
            if tx["tx_type"] == "transaction_op_type":
                reserved = tx["reserved"]
                if reserved and tx["reserved"][0] == "transfer_to":
                    # 区块链浏览器普遍使用该交易ID
                    mq_tx["Txid"] = tx["orig_trx_id"]
                    # 发起转账的地址
                    mq_tx["From"] = tx['to_contract_ledger_entry']['from_account']
                    # 转账金额
                    mq_tx["Amount"] = tx["to_contract_ledger_entry"]["amount"]["amount"]
                    # 合约地址
                    mq_tx["Contract"] = tx["to_contract_ledger_entry"]["to_account"]
                    # 合约执行函数
                    if reserved[1].find("|") > -1:
                        # 合约转账金额
                        mq_tx["Amount"] = reserved[1].split("|")[1]
                        # 实际合约转账地址
                        mq_tx["To"] = reserved[1].split("|")[0]
                    else:
                        mq_tx["To"] = reserved[1]
            else:
                # 交易id
                mq_tx["Txid"] = tx["trx_id"]
                # 发起转账的地址
                mq_tx["From"] = tx['ledger_entries'][0]['from_account']
                # 接收转账的地址
                mq_tx["To"] = tx["ledger_entries"][0]["to_account"]
                # 转账金额
                mq_tx["Amount"] = tx["ledger_entries"][0]["amount"]["amount"]
            mq_tx["status"] = "true"
            push_list.append(mq_tx)
        return push_list


class Act(TxPlugin):

    name = "act"
    desc = "解析act数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = ActParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        self.parser.rollback = rollback
        self.parser.rollback_count = rollback_count
        return self.parser.parse_block(block_num)
