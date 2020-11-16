#!/usr/bin/env python
from lib.iplugin import TxPlugin
from base58 import b58encode_check
from rpc.jsonrpc import JsonRpcRequest
from core.globals import G_CFG, G_PUSH_TEMPLATE, G_LOGGER
from lib.tool import del_0x, hex_to_int, size_convert, hex_to_bytes

__all__ = ["Neo"]


class NeoRpc(JsonRpcRequest):
    """neo rpc接口"""
    def __init__(self, cfg):
        super().__init__(cfg)

    def get_block_count(self):
        """
        获取最新高度
        :return:
        """
        method = "getblockcount"
        return self._single_post(method)

    def get_block(self, block_num):
        """
        根据区块高度获取区块详情
        :param block_num: 区块高度
        :return:
        """
        method = "getblock"
        params = [block_num, 1]
        return self._single_post(method, params)

    def get_application_log(self, tx_id):
        """
        根据交易ID获取NEP5资产交易详情
        :param tx_id: 交易ID
        :return:
        """
        method = "getapplicationlog"
        params = [tx_id, ]
        return self._single_post(method, params)

    def get_transaction(self, tx_id):
        """
        根据交易ID获取全资资产交易详情
        :param tx_id: 交易ID
        :return:
        """
        method = "getrawtransaction"
        params = [tx_id, 1, ]
        return self._single_post(method, params)


class NeoParser:
    """neo解析处理器"""
    def __init__(self):
        self.rpc = NeoRpc(G_CFG)
        self.rollback = False
        self.rollback_count = 0
        # 已推送缓存
        self.push_cache = {}

    def newest_height(self):
        """
        获取最新区块高度
        :return:
        """
        height = self.rpc.get_block_count()
        # 接口查到的区块索引，实际高度应该用区块索引减1
        return height - 1 if height else None

    def parse_block(self, block_num):
        """
        解析区块的交易列表
        :return: 返回待推送信息列表
        """
        block = self.rpc.get_block(block_num)
        txs = block.get("tx", []) if block else []
        push_list = []
        # 回溯缓存推送
        push_cache = []
        if self.rollback and block_num:
            cache_tag = self.push_cache.get(str(block_num))
            push_cache = self.push_cache.pop(str(block_num)) if cache_tag else []
        for tx in txs:
            try:
                sub_push = self.parse_tx(tx, push_cache=push_cache)
                # 每一个tx都加上区块时间和区块高度
                for push in sub_push:
                    push["BlockNumber"] = block["index"]
                    push["Time"] = block["time"]
                push_list.extend(sub_push)
            except Exception as e:
                G_LOGGER.error(f"解析交易出现异常，详情：{e}")

        len_of_cacahe = len(self.push_cache)
        G_LOGGER.info('NEO push_cache_length: {}, rollback_count: {}'.format(len_of_cacahe, self.rollback_count))
        # 非回溯类的才会缓存
        if not self.rollback:
            if len_of_cacahe <= (int(self.rollback_count) + 1):
                self.push_cache[str(block_num)] = set([tx['Txid'] for tx in push_list])
            else:
                G_LOGGER.info('NEO push_cache_length: {} out of limit, reset push_cache_dict'.format(len_of_cacahe))
                self.push_cache = {}
                self.push_cache[str(block_num)] = set([tx['Txid'] for tx in push_list])

        G_LOGGER.info("current push_cache_keys: {}".format(list(self.push_cache.keys())))

        return push_list

    def parse_tx(self, tx, push_cache=None):
        """
        解析交易详情
        :param tx: 交易详情
        :return:
        """
        if not isinstance(tx, dict):
            return []
        mq_tx = G_PUSH_TEMPLATE.copy()
        mq_tx["Txid"] = del_0x(tx["txid"])
        if self.rollback and push_cache and (mq_tx["Txid"] in push_cache):
            return []
        mq_tx["Type"] = "NEO"
        push_list = []  # 待推送交易列表
        vins = tx["vin"]
        vin_address = []
        tx_index = 0  # 同笔交易中待推送的交易索引
        for vin in vins:
            tx_id = vin["txid"]
            vout = vin["vout"]
            vin_transactions = self.rpc.get_transaction(tx_id)
            for tran in vin_transactions["vout"]:
                address = tran["address"]
                n = tran["n"]
                if vout == n:
                    vin_address.append(address)
                    break
        vouts = tx["vout"]
        for i, vout in enumerate(vouts):
            mq_tx = mq_tx.copy()
            mq_tx["From"] = vin_address[0] if len(vin_address) > 0 else ""
            mq_tx["To"] = vout["address"]
            mq_tx["status"] = "true"
            mq_tx["Amount"] = vout["value"]
            mq_tx["Contract"] = del_0x(vout["asset"])
            if vout["address"] not in vin_address:
                mq_tx["TxIndex"] = tx_index
                push_list.append(mq_tx)
                tx_index += 1
        # 如果是NEP5资产,还需要调用getapplicationlog查询交易详情
        if tx["type"] == "InvocationTransaction":
            result = self.rpc.get_application_log(tx["txid"])
            if result:
                result = result.get("executions")
                result = result[0] if result and len(result) > 0 else {}
                vmstate = result.get("vmstate")
                notifications = result.get("notifications", [])
                if vmstate and vmstate.find("FAULT") == -1:
                    for ns in notifications:
                        contract = ns.get("contract")
                        state_type = ns["state"]["type"]
                        state_value = ns["state"]["value"]
                        if state_type == "Array":
                            event_type = state_value[0]["type"]
                            event_value = state_value[0]["value"]
                            if event_type == "ByteArray":
                                event_value = bytes.fromhex(event_value).decode()
                            if event_value == "transfer":
                                from_type = state_value[1]['type']
                                from_value = state_value[1]['value']
                                to_type = state_value[2]["type"]
                                to_value = state_value[2]["value"]
                                amount_type = state_value[3]["type"]
                                amount_value = state_value[3]["value"]
                                if from_type == 'ByteArray':
                                    from_value = b58encode_check(b'\x17' + hex_to_bytes(from_value)).decode()
                                if to_type == "ByteArray":
                                    to_value = b58encode_check(b"\x17" + hex_to_bytes(to_value)).decode()
                                if amount_type == "ByteArray":
                                    amount_value = str(hex_to_int(size_convert(amount_value)))
                                mq_tx = mq_tx.copy()
                                mq_tx["From"] = from_value
                                mq_tx["To"] = to_value
                                mq_tx["status"] = "true"
                                mq_tx["Amount"] = amount_value
                                mq_tx["Contract"] = del_0x(contract)
                                if to_value not in vin_address:
                                    mq_tx["TxIndex"] = tx_index
                                    push_list.append(mq_tx)
                                    tx_index += 1
        return push_list


class Neo(TxPlugin):

    name = "neo"
    desc = "解析neo数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = NeoParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        self.parser.rollback = rollback
        self.parser.rollback_count = rollback_count
        return self.parser.parse_block(block_num)


if __name__ == '__main__':
    import json
    n = NeoParser()
    result = n.parse_block(3116306)
    print(json.dumps(result))
