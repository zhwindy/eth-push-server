#!/usr/bin/env python
import time
import os
import sys
import json
import ctypes
import platform
from lib.tool import full_url
from lib.iplugin import TxPlugin
from rpc.jsonrpc import HttpMixin
from db import redis_db
from core.globals import G_CFG, G_PUSH_TEMPLATE, G_LOGGER

__all__ = ["Xmr"]


class XmrRpc(HttpMixin):
    """
    xmr接口
    """
    def __init__(self, cfg):
        timeout = cfg.rpc.rpc_dict.get("timeout")
        super().__init__(timeout)
        self.url = cfg.rpc.rpc_dict["url"]
        self.set_json()

    def get_block_count(self):
        """
        获取最新高度
        :return:
        """
        short_url = "json_rpc"
        url = full_url(self.url, short_url)
        params = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "get_block_count",
            "params": []
        }

        res = self._single_post(url, params)
        block_count_info = res.get("result")
        return block_count_info

    def get_block(self, block_num):
        """
        根据区块高度获取区块详情
        """
        short_url = "json_rpc"
        url = full_url(self.url, short_url)
        params = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "get_block",
            "params": {
                "height": block_num
            }
        }
        res = self._single_post(url, params)
        block_info = res.get("result")
        return block_info

    def get_transactions(self, tx_ids):
        """
        查询交易
        """
        short_url = "get_transactions"
        url = full_url(self.url, short_url)
        params = {
            "txs_hashes": tx_ids,
            "decode_as_json": True
        }
        res = self._single_post(url, params)
        txs = res.get("txs", [])
        return txs


class XmrParser:
    """
    xmr解析处理器
    """
    def __init__(self):
        self.rpc = XmrRpc(G_CFG)
        self.redis = redis_db.RedisDB(G_CFG)
        self.rollback = False
        self.rollback_count = 0
        self.redis_xmr_account_key = "xmr_accounts"
        self.random_utxo_redis_key = "xmr_random_utxo"
        self.lock_time_count = 3
        self.loads_monero_lib()
        self.newest_block_height = 0

    def loads_monero_lib(self):
        """
        加载lib
        """
        os_type = platform.system()
        if os_type == 'Darwin':
            self.monero_lib = ctypes.CDLL('lib/macosx_monero.so')
        else:
            self.monero_lib = ctypes.CDLL('lib/linux_monero.so')

    def newest_height(self):
        """
        获取最新区块高度
        :return:
        """
        height = self.rpc.get_block_count()
        newest_block_height = height.get("count")
        node_height = int(newest_block_height) - self.lock_time_count
        return node_height

    def get_xmr_accounts(self):
        """
        从redis中取xmr需要扫描的账户
        """
        account_key = self.redis_xmr_account_key
        accounts_cache = self.redis.get_all_data_by_key(account_key)
        if not accounts_cache:
            return []
        accounts = [json.loads(i.decode()) for i in accounts_cache]
        return accounts

    def parse_block(self, block_height):
        """
        解析区块的交易详情
        :return: 返回待推送信息列表
        """
        accounts = self.get_xmr_accounts()
        block_detail = self.rpc.get_block(block_height)
        if not block_detail:
            return []
        tx_ids = block_detail.get("tx_hashes", [])
        if not tx_ids:
            return []
        tx_details = self.rpc.get_transactions(tx_ids)
        push_list = []
        for tx_detail in tx_details:
            tx_as_json = tx_detail.get("as_json")
            block_timestamp = tx_detail.get("block_timestamp")
            tx_global_indexes = tx_detail['output_indices']
            txid = tx_detail.get("tx_hash")
            if not txid:
                continue
            tx_json = json.loads(tx_as_json)
            extra_list = tx_json['extra']
            extra = ''.join('{:02x}'.format(x) for x in extra_list)

            extra_mem = ctypes.c_char_p()
            extra_mem.value = extra.encode()

            tx_pub_key_mem = ctypes.create_string_buffer(100)
            tx_pub_key_result = self.monero_lib.monero_from_extra_get_tx_pub_key(extra_mem, tx_pub_key_mem)
            if not tx_pub_key_result:
                continue
            # G_LOGGER.info(f"height:{block_height} txid:{txid}")
            tx_vin = tx_json.get("vin", [])
            v_in_keys = [i['key'] for i in tx_vin]
            key_images = [i.get("k_image") for i in v_in_keys]
            tx_vout = tx_json.get("vout", [])
            rct_signatures = tx_json.get("rct_signatures")
            if not rct_signatures:
                continue
            ecdhInfos = rct_signatures.get("ecdhInfo")
            if not ecdhInfos:
                continue
            outPks = rct_signatures.get("outPk")
            if not outPks:
                continue
            tx_fee = rct_signatures.get("txnFee", '0')

            mq_tx = G_PUSH_TEMPLATE.copy()
            mq_tx["Txid"] = txid
            mq_tx["Type"] = "XMR"
            mq_tx["From"] = ""
            mq_tx["To"] = ""
            mq_tx["Amount"] = ""
            mq_tx["Memo"] = ""
            mq_tx["Fee"] = str(tx_fee)
            mq_tx["Valid"] = True
            mq_tx["status"] = "true"
            mq_tx["Time"] = block_timestamp
            mq_tx["BlockNumber"] = block_height
            mq_tx["VoutsIndex"] = 0

            v_in_address = []
            for key in key_images:
                if not key:
                    continue
                redis_key = key[:6] + key[18:24] + key[-6:]
                addr_info = self.redis.get_and_delete(redis_key)
                if not addr_info:
                    continue
                addr_info = json.loads(addr_info.decode())
                G_LOGGER.info(f"height:{block_height}, txid:{txid}, key_image: {key}, addr_info: {addr_info}")
                v_in_address.append(addr_info)

            v_out_infos = []
            for v_out_index, vout in enumerate(tx_vout):
                rct = outPks[v_out_index]
                global_index = tx_global_indexes[v_out_index]
                target = vout.get("target")
                if not target:
                    continue
                v_out_address = target.get("key")
                if not v_out_address:
                    continue
                random_utxo = {
                    "global_index": str(global_index),
                    "public_key": v_out_address,
                    "rct": rct
                }
                self.redis.push_utxo_cache(self.random_utxo_redis_key, random_utxo)
                for account in accounts:
                    address = account.get("addr")
                    private_view_key = account.get("pr_vk")
                    public_spend_key = account.get("pb_sk")

                    private_view_key_mem = ctypes.c_char_p()
                    private_view_key_mem.value = private_view_key.encode()
                    public_spend_key_mem = ctypes.c_char_p()
                    public_spend_key_mem.value = public_spend_key.encode()
                    derivation_mem = ctypes.create_string_buffer(256)
                    one_time_address_mem = ctypes.create_string_buffer(128)

                    amount_echo_init = ecdhInfos[v_out_index]
                    if not amount_echo_init:
                        continue
                    amount_echo = amount_echo_init.get("amount")
                    if not amount_echo:
                        continue
                    if address and str(address).startswith("8"):
                        additional_tx_pub_key_mem = ctypes.create_string_buffer(100)
                        additional_tx_pub_key_result = self.monero_lib.monero_from_extra_get_additional_tx_pub_key(extra_mem, 0, v_out_index, additional_tx_pub_key_mem)
                        if not additional_tx_pub_key_result:
                            der_result = self.monero_lib.monero_generate_derivation_byte32(tx_pub_key_mem, private_view_key_mem, derivation_mem)
                        else:
                            der_result = self.monero_lib.monero_generate_derivation_byte32(additional_tx_pub_key_mem, private_view_key_mem, derivation_mem)
                    else:
                        der_result = self.monero_lib.monero_generate_derivation_byte32(tx_pub_key_mem, private_view_key_mem, derivation_mem)
                    if not der_result:
                        continue
                    address_result = self.monero_lib.monero_generate_one_time_public_key(derivation_mem, public_spend_key_mem, v_out_index, one_time_address_mem)
                    if not address_result:
                        continue
                    one_time_addr = (one_time_address_mem.value).decode()
                    # 判断是否命中
                    if v_out_address.strip() != one_time_addr.strip():
                        # G_LOGGER.info(f"Addr:{address}, height:{block_height}, txid:{txid}, out:{v_out_index} No")
                        continue
                    G_LOGGER.info(f"Addr:{address}, height:{block_height}, txid:{txid}, out:{v_out_index} Yes")
                    amount_echo_mem = ctypes.c_char_p()
                    amount_echo_mem.value = amount_echo.encode()
                    real_amount_mem = ctypes.create_string_buffer(128)
                    # payment_id_mem = ctypes.create_string_buffer(32)

                    amount_result = self.monero_lib.monero_decrypt_amount(derivation_mem, v_out_index, amount_echo_mem, 0, real_amount_mem)
                    amount = '0'
                    if amount_result:
                        amount = (real_amount_mem.value).decode()
                        amount = int("0x" + amount, 0)
                    mq_tx["Amount"] = amount
                    # 暂时不推送memo信息,子地址不需要memo
                    # pyamentid_result = self.monero_lib.monero_from_extra_get_payment_id(extra_mem, derivation_mem, payment_id_mem)
                    # payment_id = '0'
                    # if pyamentid_result:
                    #     payment_id = (payment_id_mem.value).decode()
                    # tmp_tx["Memo"] = payment_id
                    tmp_tx = mq_tx.copy()
                    tmp_tx["Memo"] = ""
                    tmp_tx["VoutsIndex"] = v_out_index
                    tmp_tx["To"] = address
                    v_out_infos.append(tmp_tx)
                    break
            # if (not v_in_address) or (not v_out_infos):
            #     continue
            v_in_length = len(v_in_address)
            v_out_length = len(v_out_infos)
            G_LOGGER.info(f"height:{block_height}, txid:{txid}, v_in_length:{v_in_length}, v_out_length:{v_out_length}")
            G_LOGGER.info(f"v_in_address: {v_in_address}")
            G_LOGGER.info(f"v_out_infos: {v_out_infos}")

            v_out_sort_infos = sorted(v_out_infos, key=lambda x: x["VoutsIndex"])

            max_len = max(v_out_length, v_in_length)
            for num in range(max_len):
                v_in_idx = num if num < v_in_length else -1
                v_out_idx = num if num < v_out_length else -1
                tx_tp = v_out_sort_infos[v_out_idx] if v_out_length - 1 >= v_out_idx else v_out_sort_infos[-1]
                tmp = tx_tp.copy()
                tmp['From'] = v_in_address[v_in_idx] if v_in_address else ''
                push_list.append(tmp)

        return push_list


class Xmr(TxPlugin):

    name = "xmr"
    desc = "解析xmr数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = XmrParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        return self.parser.parse_block(block_num)
