#!/usr/bin/env python
# @Time    : 18-9-18 下午5:55
# @Author  : Humingxing
# @File    : eth.py
# @DESC    : eth交易数据解析插件
import re
import time
import traceback
from lib.error import ForkError
from lib.iplugin import TxPlugin
from coin.eth.base import EtParser
from core.globals import G_PUSH_TEMPLATE, G_LOGGER, G_CFG
from lib.tool import add_0x, del_0x, int_to_hex, hex_to_int

__all__ = ["Eth"]


class EthParser(EtParser):
    """以太系交易解析处理器(支持多发送交易解析)"""

    def __init__(self, coin_type):
        super().__init__(coin_type)
        # 多签名合约标识
        self.multi_abi_len = 32 * 2
        self.multi_abi_deal = {
            'multisend': '0xaad41a41',
            'multisendToken': '0x0b66f3f5'
        }
        with open('coin/eth/MULTI_SEND_ABI_CREATE.txt', 'r') as f:
            content = f.read()
            rep = {'\n': '', '\r': '', ' ': ''}
            rep = dict((re.escape(k), v) for k, v in rep.items())
            pattern = re.compile("|".join(rep.keys()))
            self.multi_abi_deal['create'] = pattern.sub(lambda m: rep[re.escape(m.group(0))], content)

    def is_multi_send_token(self, _input):
        for k, v in self.multi_abi_deal.items():
            if _input.startswith(v):
                return k
        return False

    def parse_multi_data_pos(self, _input, _abi, pos):
        _len = self.multi_abi_len
        return int_to_hex(hex_to_int(add_0x(_input[len(_abi) + _len * pos:len(_abi) + _len * (pos + 1)])))

    def parse_multi_data(self, data):
        _len = self.multi_abi_len
        data_count = int(len(data) / _len)
        return [add_0x(data[_len * i: _len * (i + 1)][24:]) for i in range(data_count)]

    def parse_block(self, block_num):
        """
        解析区块的交易列表(支持多发送)
        :return: 返回待推送信息列表
        """
        while True:
            try:
                block = self.rpc.eth_get_block_by_number(block_num)
                if block is None:
                    raise Exception(f"获取到最新高度{block_num}区块详情为None，触发异常，尝试重新获取")
                G_LOGGER.info(f"当前推送的区块：高度{int(block.get('number', '0x0'), 16)}，哈希{block.get('hash', '')}")
                self._check_uncle(block)
                # 遍历交易
                push_list = []
                if block.get("transactions") is None:
                    time.sleep(3)
                    continue
                tx_hashes = [tx["hash"] for tx in block["transactions"]]
                receipts = self.rpc.eth_get_transaction_receipt(tx_hashes) if tx_hashes else []
                assert len(receipts) == len(block["transactions"])
                for i, tx in enumerate(block["transactions"]):
                    _input = tx.get('input', '')
                    _to = tx.get('to', '')
                    _from = tx.get('from', '')
                    multi_send_token = self.is_multi_send_token(_input)
                    mq_tx = G_PUSH_TEMPLATE.copy()
                    mq_tx["BlockNumber"] = hex_to_int(tx["blockNumber"])
                    mq_tx["Txid"] = tx["hash"]
                    mq_tx["Type"] = self.coin_type
                    mq_tx["From"] = tx["from"]
                    mq_tx["Time"] = block["timestamp"]
                    is_token_transfer = self._is_token(_input)
                    # 交易状态
                    mq_tx["Valid"] = self.get_status(receipts[i], is_token_transfer)
                    mq_tx["status"] = self.get_tx_status(receipts[i], is_token_transfer)
                    # 手续费
                    gas_price = tx.get("gasPrice", "0x0")
                    gas_used = receipts[i].get("gasUsed", "0x0") if receipts[i] else "0x0"
                    mq_tx["Fee"] = hex(int(gas_price, 16) * int(gas_used, 16))

                    if is_token_transfer:
                        if _from != _to:
                            tx['contract'] = _to
                        _to = self._get_token_to_address(_input)
                        _value = self._get_token_to_value(_input)
                        tx["to"] = _to if _to != "0x" else f"0x{'0'*40}"
                        tx["value"] = _value if _value != "0x" else "0x0"
                    elif multi_send_token:
                        continue
                        _len = self.multi_abi_len
                        _del_0x_input = del_0x(_input)
                        if multi_send_token == 'create':
                            # 多发送合约创建的交易，保存到数据库存起来
                            data = dict()
                            data['contract'] = tx['contract'] = tx['creates']
                            data['blockNumber'] = hex_to_int(tx["blockNumber"])
                            data['timestamp'] = block["timestamp"]
                            data['hash'] = tx["hash"]
                            # 保存合约
                            self.db.mysql.contract.insert_contract(data)
                            G_LOGGER.info(f"发现新的多发送合约{data['contract']}并保存")
                            continue
                        else:
                            token = ''
                            address_list = amount_list = []
                            # 不是我们自己的多发送合约则不处理
                            contract = self.db.mysql.contract.get_by_contract(tx["to"])
                            if not contract:
                                continue
                            if multi_send_token == 'multisend':
                                _multisend_abi = del_0x(self.multi_abi_deal['multisend'])
                                result = self.parse_multi_data(_del_0x_input[len(_multisend_abi):])
                                address_count = hex_to_int(result[2])
                                address_list = result[3:3+address_count]
                                amount_count = hex_to_int(result[3+address_count])
                                amount_list = result[4+address_count:4+address_count+amount_count]
                            elif multi_send_token == 'multisendToken':
                                _multisend_token_abi = del_0x(self.multi_abi_deal['multisendToken'])
                                token = add_0x(self.parse_multi_data_pos(_del_0x_input, _multisend_token_abi, 0))
                                result = self.parse_multi_data(_del_0x_input[len(_multisend_token_abi):])
                                address_count = hex_to_int(result[3])
                                address_list = result[4:4 + address_count]
                                amount_count = hex_to_int(result[4 + address_count])
                                amount_list = result[5 + address_count:5 + address_count + amount_count]
                            for k, v in enumerate(address_list):
                                mq_tx = mq_tx.copy()
                                mq_tx["To"] = v
                                mq_tx["Amount"] = int_to_hex(hex_to_int(amount_list[k]))
                                mq_tx["Contract"] = token
                                push_list.append(mq_tx)
                            return push_list
                    if tx["to"]:
                        mq_tx["To"] = tx["to"]
                        mq_tx["Amount"] = tx["value"]
                        if 'contract' in tx.keys():
                            mq_tx["Contract"] = tx["contract"]
                        push_list.append(mq_tx)
                return push_list
            except ForkError as ex:
                raise ForkError(ex.height, ex.msg)
            except Exception as ex:
                traceback.print_exc()
                G_LOGGER.info(f"获取块出现异常，尝试重新获取。异常原因：{str(ex)}")
            time.sleep(10)


class Eth(TxPlugin):

    name = "eth"
    desc = "解析eth数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = EthParser("ETH")

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        self.parser.rollback = rollback
        self.parser.rollback_count = rollback_count
        return self.parser.parse_block(block_num)


if __name__ == "__main__":
    import json
    e = Eth()
    b = 8476212
    while True:
        r = e.push_list(b)
        b += 1
        time.sleep(1)
        print(json.dumps(r))
