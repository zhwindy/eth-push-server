#!/usr/bin/env python
# @Time    : 19-1-3 下午4:06
# @Author  : Humingxing
# @File    : eth_multi.py
# @DESC    : eth多签名交易数据解析插件
import re
import time
import struct
import traceback
from lib.error import ForkError
from lib.iplugin import TxPlugin
from coin.eth.base import EtParser
from core.globals import G_ETH_MULTI_PUSH_TEMPLATE, G_LOGGER, G_CFG
from lib.tool import add_0x, del_0x, int_to_hex, hex_to_int, sha, get_now

__all__ = ["EthMulti"]


class EthMultiParser(EtParser):
    """以太系多签名交易解析处理器"""

    def __init__(self, coin_type):
        super().__init__(coin_type)
        # 多签名合约标识
        self.multi_abi_len = 32 * 2
        self.multi_abi_deal = {
            'execute': '0xb61d27f6',
            'confirm': '0x797af627',
            'revoke': '0xb75c7dc6',
            'addOwner': '0x7065cb48',
            'removeOwner': '0x173825d9',
            'changeOwner': '0xf00d4b5d',
            'changeRequirement': '0xba51a6df',
            'setDailyLimit': '0xb20d30a9',
            'create': self._read_abi_txt('coin/eth/MULTI_ABI_CREATE.txt'),
            'create2': self._read_abi_txt('coin/eth/MULTI_ABI_CREATE_2.txt'),
        }

    def _read_abi_txt(self, path):
        with open(path, 'r') as f:
            content = f.read()
            rep = {'\n': '', '\r': '', ' ': ''}
            rep = dict((re.escape(k), v) for k, v in rep.items())
            pattern = re.compile("|".join(rep.keys()))
            return pattern.sub(lambda m: rep[re.escape(m.group(0))], content)

    def is_multi_token(self, _input):
        for k, v in self.multi_abi_deal.items():
            if _input.startswith(v):
                return k
        return False

    def parse_multi_data_pos(self, _input, _abi, pos):
        _len = self.multi_abi_len
        return int_to_hex(hex_to_int(add_0x(_input[len(_abi)+_len*pos:len(_abi)+_len*(pos+1)])))

    def parse_multi_data(self, data):
        _len = self.multi_abi_len
        data_count = int(len(data) / _len)
        return [add_0x(data[_len * i: _len * (i + 1)][24:]) for i in range(data_count)]

    def parse_block(self, block_num):
        """
        解析区块的交易列表
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
                for tx in block["transactions"]:
                    _input = tx.get('input', '')
                    _del_0x_input = del_0x(_input)
                    _to = tx.get('to', '')
                    _from = tx.get('from', '')
                    _len = self.multi_abi_len
                    multi_token = self.is_multi_token(_input)
                    if multi_token:
                        if multi_token in ['create', 'create2']:
                            data = dict()
                            if multi_token == 'create':
                                _create_abi = del_0x(self.multi_abi_deal['create'])
                            else:
                                _create_abi = del_0x(self.multi_abi_deal['create2'])
                            data['contract'] = tx['creates']
                            data['needSign'] = self.parse_multi_data_pos(_del_0x_input, _create_abi, 0)
                            data['dayLimit'] = self.parse_multi_data_pos(_del_0x_input, _create_abi, 1)
                            data['maxSign'] = self.parse_multi_data_pos(_del_0x_input, _create_abi, 2)
                            owners = self.parse_multi_data(_del_0x_input[len(_create_abi) + _len * 3:])
                            data['owners'] = '|'.join(owners)
                            data['timestamp'] = block['timestamp']
                            data['hash'] = tx['hash']
                            data['blockNumber'] = tx['blockNumber']
                            # 保存合约
                            self.db.mysql.eth_multi_contract.insert_contract(data)
                            G_LOGGER.info(f"发现新的多签名合约{data['contract']}并保存")
                        else:
                            contract = self.db.mysql.eth_multi_contract.get_by_contract(_to)
                            if not contract:
                                continue
                            need_sign = hex_to_int(contract.needSign)
                            max_sign = hex_to_int(contract.maxSign)
                            owners = contract.owners.split('|')
                            data = dict()
                            operation_id = ''
                            data['timestamp'] = block['timestamp']
                            data['hash'] = tx['hash']
                            data['blockNumber'] = tx['blockNumber']
                            data['contract'] = _to
                            data['from_addr'] = _from
                            data['to_addr'] = data['value'] = data['data'] = ''
                            if multi_token == 'execute':
                                _execute_abi = del_0x(self.multi_abi_deal['execute'])
                                data['to_addr'] = self.parse_multi_data_pos(_del_0x_input, _execute_abi, 0)
                                data['value'] = self.parse_multi_data_pos(_del_0x_input, _execute_abi, 1)
                                data_tmp = self.parse_multi_data(_del_0x_input[len(_execute_abi) + _len * 2:])
                                data['data'] = '|'.join(data_tmp)
                                # 计算出operationId
                                block_number = struct.pack('>QQQQ', 0, 0, 0, hex_to_int(tx['blockNumber']))
                                operation_id = add_0x(sha(*[bytes.fromhex(_del_0x_input), block_number]))
                            elif multi_token == 'confirm':
                                _confirm_abi = del_0x(self.multi_abi_deal['confirm'])
                                operation_id = add_0x(self.parse_multi_data_pos(_del_0x_input, _confirm_abi, 0))
                            elif multi_token == 'revoke':
                                _revoke_abi = del_0x(self.multi_abi_deal['revoke'])
                                operation_id = add_0x(self.parse_multi_data_pos(_del_0x_input, _revoke_abi, 0))

                            data['operationId'] = operation_id
                            data['type'] = multi_token
                            # 判断是否已经有交易hash，没有才更新多签名交易确认数和拒绝数，
                            # 如果已经有该交易了说明是因为回滚，回滚操作时不需要更新确认数和拒绝数
                            is_rollback = False
                            if self.db.mysql.trade.get_by_hash(tx['hash']):
                                is_rollback = True
                                # 更新已经保存交易的区块高度
                                self.db.mysql.trade.update_trade_block_number(data)
                                G_LOGGER.info(f"发现重复的交易{tx['hash']}，该交易不推送，不计算确认数和拒绝数。")
                            else:
                                # 保存交易(operation_id不一致的交易也应该保存，方便查看历史)
                                self.db.mysql.trade.insert_trade(data)

                            # 更新多签名交易确认数和拒绝数
                            if multi_token == 'confirm' and not is_rollback:
                                # 确认数+1
                                result = self.db.mysql.trade.get_by_operation_id(operation_id, _to)
                                if not result:
                                    continue
                                data['confirm'] = result.confirm + 1
                                data['revoke'] = result.revoke
                                data['done'] = result.done
                                # 更新确认数
                                if not result.done:
                                    # 判断确认数是否达到最大
                                    if data['confirm'] == need_sign:
                                        data['done'] = 1
                                        # 更新当前这笔确认交易done为1
                                        self.db.mysql.trade.update_done_by_tx_hash(1, tx['hash'])
                                    self.db.mysql.trade.update_execute_by_operation_id(data, operation_id, _to)
                            elif multi_token == 'revoke' and not is_rollback:
                                # 拒绝数+1
                                result = self.db.mysql.trade.get_by_operation_id(operation_id, _to)
                                if not result:
                                    continue
                                data['confirm'] = result.confirm
                                data['revoke'] = result.revoke + 1
                                data['done'] = result.done
                                # 更新拒绝数
                                if not result.done:
                                    # 判断拒绝数是否达到最大
                                    if data['revoke'] == need_sign:
                                        data['done'] = 2
                                        # 更新当前这笔确认交易done为2
                                        self.db.mysql.trade.update_done_by_tx_hash(2, tx['hash'])
                                    self.db.mysql.trade.update_execute_by_operation_id(data, operation_id, _to)

                            # 推送
                            mq_tx = G_ETH_MULTI_PUSH_TEMPLATE.copy()
                            mq_tx['operationId'] = operation_id
                            mq_tx['Txid'] = tx.get('hash', '')
                            mq_tx['owners'] = owners
                            mq_tx['isPayer'] = 1 if multi_token == 'execute' else 0
                            mq_tx['from'] = _from
                            mq_tx['to'] = data.get('to_addr', '')
                            mq_tx['multContract'] = _to
                            mq_tx['signN'] = need_sign
                            mq_tx['maxN'] = max_sign
                            mq_tx['height'] = hex_to_int(data['blockNumber']) if data.get('blockNumber') else 0
                            mq_tx['time'] = data.get('timestamp', '')
                            result = self.db.mysql.trade.get_by_operation_id(operation_id, _to)
                            if result:
                                mq_tx['value'] = result.value
                                mq_tx['confirmed'] = result.done
                            if multi_token == 'execute':
                                mq_tx['codeType'] = '001'
                            elif multi_token == 'confirm':
                                mq_tx['codeType'] = '002'
                            elif multi_token == 'revoke':
                                mq_tx['codeType'] = '003'
                            elif multi_token == 'addOwner':
                                mq_tx['codeType'] = '004'
                            elif multi_token == 'removeOwner':
                                mq_tx['codeType'] = '005'
                            elif multi_token == 'changeOwner':
                                mq_tx['codeType'] = '006'
                            elif multi_token == 'changeRequirement':
                                mq_tx['codeType'] = '007'
                            elif multi_token == 'setDailyLimit':
                                mq_tx['codeType'] = '008'
                            mq_tx["TxIndex"] = 0
                            # 查询该交易有没有推送，如果已经推送则不再重复推送
                            result = self.db.mysql.trade.get_by_hash(mq_tx['Txid'])
                            push_data = dict()
                            push_data['operationId'] = operation_id
                            push_data['timestamp'] = block['timestamp']
                            push_data['blockNumber'] = tx['blockNumber']
                            push_data['is_push'] = result.is_push
                            push_data['push_time'] = result.push_time
                            if result and not result.is_push:
                                push_list.append(mq_tx)
                                # 更新推送状态
                                push_data['is_push'] = 1
                                push_data['push_time'] = get_now()
                            self.db.mysql.trade.update_by_tx_hash(push_data, mq_tx['Txid'])
                # 缓存已推送的区块高度和hash
                self.db.redis.save_cache_block(hex_to_int(block["number"]), block["hash"])
                # 获取要推送的交易的手续费和交易状态
                tx_hashes = [p["Txid"] for p in push_list]
                txs = self.rpc.eth_get_transaction_by_hash(tx_hashes) if tx_hashes else []
                receipts = self.rpc.eth_get_transaction_receipt(tx_hashes) if tx_hashes else []
                assert len(receipts) == len(push_list)
                for i, p in enumerate(push_list):
                    # 交易状态
                    _input = txs[i].get('input', '')
                    is_token_transfer = self._is_token(_input)
                    p["Valid"] = self.get_status(receipts[i], is_token_transfer)
                    # 手续费
                    gas_price = txs[i].get("gasPrice", "0x0")
                    gas_used = receipts[i].get("gasUsed", "0x0") if receipts[i] else "0x0"
                    p["Fee"] = hex(int(gas_price, 16) * int(gas_used, 16))
                return push_list
            except ForkError as ex:
                raise ForkError(ex.height, ex.msg)
            except Exception as e:
                traceback.print_exc()
                G_LOGGER.info(f"获取块出现异常，尝试重新获取。异常原因：{str(e)}")
            time.sleep(10)


class EthMulti(TxPlugin):

    name = "eth_multi"
    desc = "解析eth多签名交易数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = EthMultiParser("ETH")

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        self.parser.rollback = rollback
        self.parser.rollback_count = rollback_count
        return self.parser.parse_block(block_num)


if __name__ == '__main__':
    e = EthMulti()
    r = e.push_list(6877709)
    import json
    print(json.dumps(r))
