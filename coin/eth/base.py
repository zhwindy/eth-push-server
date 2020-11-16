#!/usr/bin/env python
# @Time    : 18-10-9 上午10:24
# @Author  : Humingxing
# @File    : base.py
# @DESC    : 以太系币种基类
import time
import traceback
from db.dbs import Db
from lib.error import ForkError
from rpc.jsonrpc import JsonRpcRequest
from lib.tool import int_to_hex, hex_to_int, add_0x
from core.globals import G_CFG, G_PUSH_TEMPLATE, G_LOGGER


class EtRpc(JsonRpcRequest):
    """以太系rpc接口"""
    def __init__(self, cfg):
        super().__init__(cfg)

    def _get_params(self, ps, *args, to_hex=True):
        if isinstance(ps, (list, tuple, set, range)):
            if to_hex:
                params = [[int_to_hex(p), *args] for p in ps]
            else:
                params = [[p, *args] for p in ps]
        else:
            if to_hex:
                params = [int_to_hex(ps), *args]
            else:
                params = [ps, *args]
        return params

    def eth_block_number(self):
        """
        获取最新高度
        :return:
        """
        method = "eth_blockNumber"
        return hex_to_int(self._single_post(method))

    def eth_get_block_by_number(self, height, offset=0, tx_detail=True):
        """
        根据区块高度获取区块详情
        :param height: 区块高度
        :param offset: 一次拿多少区块
        :param tx_detail: 是否返回交易详情
        :return:
        """
        method = 'eth_getBlockByNumber'
        func = self._get_func(offset)
        if offset:
            height = range(height, height + offset + 1)
        params = self._get_params(height, tx_detail)
        result = func(method, params)
        return result

    def eth_get_block_by_hash(self, block_hash, tx_detail=True):
        """
        根据区块hash获取区块详情
        :param block_hash: 区块高度
        :param tx_detail: 是否返回交易详情
        :return:
        """
        method = 'eth_getBlockByHash'
        func = self._get_func(0)
        params = [block_hash, tx_detail]
        result = func(method, params)
        return result

    def eth_get_code(self, address, height='latest'):
        method = 'eth_getCode'
        func = self._get_func(address)
        params = self._get_params(address, height, to_hex=False)
        result = func(method, params)
        return result

    def eth_get_transaction_receipt(self, tx_hashes):
        method = 'eth_getTransactionReceipt'
        func = self._get_func(tx_hashes)
        params = self._get_params(tx_hashes, to_hex=False)
        result = func(method, params)
        return result

    def eth_get_transaction_by_hash(self, tx_hashes):
        method = 'eth_getTransactionByHash'
        func = self._get_func(tx_hashes)
        params = self._get_params(tx_hashes, to_hex=False)
        result = func(method, params)
        return result


class EtParser:
    """
    以太系币种解析处理器
    """
    def __init__(self, coin_type):
        self.rpc = EtRpc(G_CFG)
        self.db = Db(G_CFG)
        self.abi_deal = "0xa9059cbb000000000000000000000000"
        self.address_len = 20 * 2
        self.coin_type = coin_type
        self.rollback = False
        self.rollback_count = 0
        # 已推送缓存
        self.push_cache = {}

    def newest_height(self):
        """
        获取最新区块高度
        :return:
        """
        # 延迟3个块获取
        return self.rpc.eth_block_number() - 3

    def _is_token(self, _input):
        if _input.startswith(self.abi_deal):
            return True
        return False

    def _get_token_to_value(self, _input):
        return add_0x(_input[len(self.abi_deal) + self.address_len:])

    def _get_token_to_address(self, _input):
        return add_0x(_input[len(self.abi_deal):self.address_len + len(self.abi_deal)])

    def _check_uncle(self, block):
        """
        检查当前区块的叔块，看是否需要回滚
        """
        # 检查当前传入的区块高度和最新的区块高度是否一致，相差大于100说明当前是处理比较老的区块了则不用检查叔块
        current_height = hex_to_int(block["number"])
        newest_height = self.rpc.eth_block_number()
        if newest_height - current_height > 100:
            return False
        # 没有叔块也不用检查叔块
        uncles = block["uncles"]
        if not uncles:
            return False
        G_LOGGER.info(f"区块{current_height}发现叔块:{','.join(uncles)}")
        # 获取叔块对应最小区块高度的hash
        uncle_height = []
        for uncle in uncles:
            uncle_block = self.rpc.eth_get_block_by_hash(uncle, tx_detail=False)
            if uncle_block:
                uncle_height.append(hex_to_int(uncle_block["number"]))
        if uncle_height:
            min_height = min(uncle_height)
            # 查询缓存的区块中是否有uncle hash
            push_block_hash = self.db.redis.get_cache_block(min_height)
            # 已经推送的该高度对应的区块hash如果不在uncles中，说明之前推送的区块就是正确的区块，不用继续处理
            if push_block_hash not in uncles:
                return False
            raise ForkError(min_height, f"{min_height}高度同步过程中出现临时分叉")

    def parse_block(self, block_num):
        """
        解析区块的交易列表
        """
        try:
            block = self.rpc.eth_get_block_by_number(block_num)
            if block is None:
                raise Exception(f"获取区块高度{block_num}区块详情为None，触发异常，尝试重新获取")
            G_LOGGER.info(f"当前推送的区块：高度{int(block.get('number', '0x0'), 16)}，哈希{block.get('hash', '')}")
            self._check_uncle(block)
            # 遍历交易
            push_list = []
            if block.get("transactions") is None:
                raise Exception(f"获取区块高度{block_num}区块transactions为None，触发异常，尝试重新获取")
            tx_hashes = [tx["hash"] for tx in block["transactions"]]
            receipts = self.rpc.eth_get_transaction_receipt(tx_hashes) if tx_hashes else []
            assert len(receipts) == len(block["transactions"])
            for i, tx in enumerate(block["transactions"]):
                _input = tx.get('input', '')
                _to = tx.get('to', '')
                _from = tx.get('from', '')
                is_token_transfer = self._is_token(_input)
                if is_token_transfer:
                    if _from != _to:
                        tx['contract'] = _to
                    _to = self._get_token_to_address(_input)
                    _value = self._get_token_to_value(_input)
                    tx["to"] = _to if _to != "0x" else f"0x{'0'*40}"
                    tx["value"] = _value if _value != "0x" else "0x0"
                mq_tx = G_PUSH_TEMPLATE.copy()
                mq_tx["BlockNumber"] = int(tx["blockNumber"], 16)
                mq_tx["Txid"] = tx["hash"]
                mq_tx["Type"] = self.coin_type
                mq_tx["From"] = tx["from"]
                # 排除to为空的
                if not tx["to"]:
                    continue
                mq_tx["To"] = tx["to"]
                mq_tx["Time"] = block["timestamp"]
                mq_tx["Amount"] = tx["value"]
                # 手续费
                gas_price = tx.get("gasPrice", "0x0")
                gas_used = receipts[i].get("gasUsed", "0x0") if receipts[i] else "0x0"
                mq_tx["Fee"] = hex(int(gas_price, 16) * int(gas_used, 16))
                # 交易状态
                mq_tx["Valid"] = self.get_status(receipts[i], is_token_transfer)
                mq_tx["status"] = self.get_tx_status(receipts[i], is_token_transfer)
                if 'contract' in tx.keys():
                    mq_tx["Contract"] = tx["contract"]
                push_list.append(mq_tx)
            # 缓存已推送的区块高度和hash
            self.db.redis.save_cache_block(hex_to_int(block["number"]), block["hash"])
            return push_list
        except ForkError as ex:
            raise ForkError(ex.height, ex.msg)
        except Exception as e:
            traceback.print_exc()
            G_LOGGER.info(f"获取块出现异常，尝试重新获取。异常原因：{str(e)}")

    def get_status(self, tx_receipt, is_token_transfer):
        """
        获取交易状态
        :param tx_receipt: 交易票据
        :param is_token_transfer: 是否是token转账交易
        :return:
        """
        # 一、优先判断非合约交易和早期交易，直接根据status判断即可
        # 二、如果是token的transfer交易，则继续根据eth_getTransactionReceipt获取的票据信息，判断logs，以下三种情况认定失败：
        # 1、logs为空；2、logs里面读取topics信息不包含transfer的事件名字；3、topics和data不是标准的erc20日志
        result = False
        status = tx_receipt.get("status")
        if status == "0x1" or (status is None and not is_token_transfer):
            # 优先判断status，值为0x1的默认认为是成功交易
            # etc交易和早期eth也没有status信息（获取的值为None，且非token的交易logs没内容，只能通过status判断）
            result = True
        if is_token_transfer:
            # 如果是token的transfer交易，则进一步判断logs
            logs = tx_receipt.get("logs", [])
            if not logs:
                # 日志为空，判定失败
                result = False
            else:
                # 日志不为空，继续检查日志topics和data对应关系,尝试过解析from，to和amount，但在不同合约中的表现形式都不完全一样，不好判断
                is_transfer = False  # 是否是合法的transfer日志
                for log in logs:
                    data = log.get("data", "")
                    topics = log.get("topics", [])
                    if topics[0] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" and \
                            len(data) == 2 + (4 - len(topics)) * 64:
                        is_transfer = True
                if not is_transfer:
                    result = False
        return result

    def get_tx_status(self, tx_receipt, is_token_transfer):
        """
        获取交易状态
        :param tx_receipt: 交易票据
        :param is_token_transfer: 是否是token转账交易
        :return:
        """
        # 一、优先判断非合约交易和早期交易，直接根据status判断即可
        # 二、如果是token的transfer交易，则继续根据eth_getTransactionReceipt获取的票据信息，判断logs，以下三种情况认定失败：
        # 1、logs为空；2、logs里面读取topics信息不包含transfer的事件名字；3、topics和data不是标准的erc20日志
        result = "unconfirm"
        status = tx_receipt.get("status")
        if status == "0x1" or (status is None and not is_token_transfer):
            # 优先判断status，值为0x1的默认认为是成功交易
            # etc交易和早期eth也没有status信息（获取的值为None，且非token的交易logs没内容，只能通过status判断）
            result = "true"
        if is_token_transfer:
            # 如果是token的transfer交易，则进一步判断logs
            logs = tx_receipt.get("logs", [])
            if not logs:
                # 日志为空，判定失败
                result = "false"
            else:
                # 日志不为空，继续检查日志topics和data对应关系,尝试过解析from，to和amount，但在不同合约中的表现形式都不完全一样，不好判断
                is_transfer = False  # 是否是合法的transfer日志
                for log in logs:
                    data = log.get("data", "")
                    topics = log.get("topics", [])
                    if topics[0] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" and (len(data) == 2 + (4 - len(topics)) * 64):
                        is_transfer = True
                if not is_transfer:
                    result = "false"
        return result
