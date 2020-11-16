#!/usr/bin/env python
import re
import struct
import string
import time
import bitcoin
import traceback
from decimal import Decimal
from struct import Struct
from rpc.jsonrpc import JsonRpcRequest
from core.globals import G_CFG, G_PUSH_TEMPLATE, G_LOGGER
from lib.tool import seperate_big_list


class BtcRpcBase(JsonRpcRequest):
    """
    BTC系列币种基类
    """

    def __init__(self, cfg):
        super().__init__(cfg)

    def get_block_count(self):
        """
        获取区块数量
        """
        method = "getblockcount"
        params = []

        return self._single_post(method, params=params)

    def get_block_hash(self, block_height):
        """
        获取区块哈希
        :param block_height 区块高度
        """
        method = "getblockhash"
        params = [block_height]

        return self._single_post(method, params=params)

    def get_block(self, block_hash):
        """
        获取区块
        :param block_hash 区块哈希
        """
        method = "getblock"
        params = [block_hash]

        return self._single_post(method, params=params)

    def get_transaction(self, tx_id):
        """
        获取单笔交易
        :param tx_id 交易ID
        """
        method = "getrawtransaction"
        params = [tx_id, 1]

        return self._single_post(method, params=params)

    def get_getrawmempool(self):
        """
        获取mempool中所有未确认的交易
        """
        method = "getrawmempool"
        params = []

        return self._single_post(method, params=params)

    def get_transactions(self, tx_ids):
        method = "getrawtransaction"
        params = [(tx_id, 1) for tx_id in tx_ids]

        return self._many_post(method, params)

    def omni_get_transaction(self, tx_id):
        method = "omni_gettransaction"
        params = [tx_id]

        return self._single_post(method, params=params)

    def omni_get_transactions(self, tx_ids):
        method = "omni_gettransaction"
        params = [(tx_id) for tx_id in tx_ids]

        return self._many_post(method, params)


class BtcParserBase:
    """
    btc系列币种解析器基类
    """

    def __init__(self):
        self.rpc = BtcRpcBase(G_CFG)
        self.current_height = 0
        self.coin_type = None
        self.invalid_tx_type_list = []
        self.segwit_tx_type_list = []
        self.set_invalid_tx_types()
        self.set_sigwit_tx_types()
        # 比特币数量单位小数点数位
        self.coin_value_unit = 8
        # 每次从节点请求的tx笔数
        self.request_chunk_size = 50
        # 已推送缓存
        self.push_cache = {}

    def set_invalid_tx_types(self):
        """
        设置不合法交易类型
        """
        self.invalid_tx_type_list = ['nulldata', 'nullData', 'Nulldata', "nonstandard", "call", "create"]

    def set_sigwit_tx_types(self):
        """
        设置隔离见证交易类型,不同币种可按实际情况重载
        """
        self.segwit_tx_type_list = ["witness_v0_keyhash"]

    def newest_height(self):
        """
        获取当前最新高度
        """
        height = self.rpc.get_block_count()

        return height

    def parse_block(self, block_height):
        """
        解析区块
        """
        if not isinstance(block_height, int):
            block_height = int(block_height)

        block_hash = self.rpc.get_block_hash(block_height)
        if not block_hash:
            return []

        self.current_height = block_height

        block = self.rpc.get_block(block_hash)

        txs = block['tx'] if block else []
        if not txs:
            return []

        push_list = self.parse_txs(txs, height=block_height)

        len_of_cacahe = len(self.push_cache)
        G_LOGGER.info('{} push_cache_length: {}, rollback_count: {}'.format(self.coin_type, len_of_cacahe, self.rollback_count))
        # 非回溯类的才会缓存
        if not self.rollback:
            if len_of_cacahe <= (int(self.rollback_count) + 1):
                self.push_cache[str(block_height)] = set([tx['Txid'] for tx in push_list])
            else:
                G_LOGGER.info('{} push_cache_length: {} out of limit, reset push_cache_dict'.format(self.coin_type, len_of_cacahe))
                self.push_cache = {}
                self.push_cache[str(block_height)] = set([tx['Txid'] for tx in push_list])

        G_LOGGER.info("current push_cache_keys: {}".format(list(self.push_cache.keys())))

        return push_list

    def parse_txs(self, txs, height=None):
        """
        解析区块内交易
        """
        push_list = []
        push_cache = []
        if self.rollback and height:
            cache_tag = self.push_cache.get(str(height))
            push_cache = self.push_cache.pop(str(height)) if cache_tag else []

        G_LOGGER.info('Start process block:{}, rollback_status:{}, rollback_count: {}, cache_data_len:{}, block txcounts:{}'.format(height, self.rollback, self.rollback_count, len(push_cache), len(txs)))
        coinbase_txs = self.parse_coinbase_tx(txs.pop(0), push_cache=push_cache)

        total_fees = 0
        process_tx_counts = 0
        for tx_ids in seperate_big_list(txs, chunk=self.request_chunk_size):
            tx_details = self.rpc.get_transactions(tx_ids)
            # G_LOGGER.info('Block:{}, current process tx counts:{}'.format(height, len(tx_details)))
            if not tx_details:
                continue
            process_tx_counts += len(tx_details)
            txs_list, fees = self.parse_normal_tx(tx_details, push_cache=push_cache)
            total_fees += fees
            push_list.extend(txs_list)

        # 给coinbase交易添加fee字段值
        for tx in coinbase_txs:
            tx['Fee'] = str(total_fees)
            push_list.append(tx)

        G_LOGGER.info('Block:{}, process txcounts:{}, push txcounts: {}'.format(height, process_tx_counts + 1, len(push_list)))

        return push_list

    def parse_coinbase_tx(self, tx_id, mempool_tx=False, push_cache=None):
        """
        解析封装币基交易
        """
        if self.rollback and push_cache and (tx_id in push_cache):
            return []

        coinbase_list = []
        tx_detail = self.rpc.get_transaction(tx_id)

        mq_tx = G_PUSH_TEMPLATE.copy()
        mq_tx["Txid"] = tx_id
        mq_tx["Type"] = self.coin_type
        mq_tx["From"] = None
        mq_tx["Time"] = int(tx_detail["time"])
        mq_tx["BlockNumber"] = self.current_height

        total_out_value = 0
        coinbase_v_outs = tx_detail['vout']
        for v_out in coinbase_v_outs:
            tx_type = self.get_transaction_type(v_out)
            if not tx_type:
                continue
            v_out_address = self.get_vout_address(tx_type, v_out)
            if not v_out_address:
                continue
            v_out_value = self.get_handle_value(v_out)
            total_out_value += v_out_value
            tmp = mq_tx.copy()
            tmp["To"] = v_out_address
            tmp["Amount"] = str(v_out_value)
            tmp["VoutsIndex"] = v_out['n']
            tmp["status"] = "true"
            coinbase_list.append(tmp)

        return coinbase_list

    def parse_normal_tx(self, tx_details, mempool_tx=False, push_cache=None):
        """
        解析封装普通交易
        """
        push_list = []
        total_fees = 0
        for tx_detail in tx_details:
            mq_tx = G_PUSH_TEMPLATE.copy()
            mq_tx["Txid"] = tx_detail['txid']
            if self.rollback and push_cache and (mq_tx["Txid"] in push_cache):
                continue
            mq_tx["Type"] = self.coin_type
            mq_tx["status"] = "true"
            if mempool_tx:
                mq_tx["Time"] = int(time.time())
                mq_tx["BlockNumber"] = 0
            else:
                mq_tx["Time"] = int(tx_detail["time"])
                mq_tx["BlockNumber"] = self.current_height

            v_in_list = tx_detail['vin']
            v_in_addresses = []
            v_in_infos = []
            for v_in in v_in_list:
                scriptSig = v_in.get('scriptSig')
                txid = v_in.get('txid')
                if txid:
                    v_in_infos.append(dict(txid=txid, index=v_in.get('vout', 0)))
                if not scriptSig:
                    continue
                v_in_address = self.get_address_from_scriptsig(scriptSig['hex'])
                if not v_in_address:
                    continue
                v_in_addresses.append(v_in_address)

            v_out_list = tx_detail['vout']
            out_infos = []
            total_out_value = 0
            for v_out in v_out_list:
                info = {}
                v_out_value = self.get_handle_value(v_out)
                total_out_value += v_out_value

                tx_type = self.get_transaction_type(v_out)
                if not tx_type:
                    continue
                v_out_address = self.get_vout_address(tx_type, v_out)
                if not v_out_address:
                    continue
                v_out_index = v_out['n']
                if v_out_address in v_in_addresses:
                    # 标记找零
                    info['Charge'] = True
                info['To'] = v_out_address
                info['Amount'] = str(v_out_value)
                info["VoutsIndex"] = v_out_index
                out_infos.append(info)
            # 计算交易手续费
            tx_fee = self.get_tx_fee(v_in_infos, total_out_value)
            total_fees += tx_fee
            # 到此只缺FROM
            if not out_infos:
                continue
            v_in_length = len(v_in_addresses)
            v_out_length = len(out_infos)

            v_out_infos = sorted(out_infos, key=lambda x: x["VoutsIndex"])

            max_len = max(v_out_length, v_in_length)
            for num in range(max_len):
                v_in_idx = num if num < v_in_length else -1
                v_out_idx = num if num < v_out_length else -1

                tx_tp = mq_tx.copy()
                tx_tp.update(v_out_infos[v_out_idx]) if v_out_length - 1 >= v_out_idx else tx_tp.update(v_out_infos[-1])
                tx_tp['From'] = v_in_addresses[v_in_idx] if v_in_addresses else ''
                tx_tp['Fee'] = str(tx_fee)
                push_list.append(tx_tp)

        return push_list, total_fees

    def get_tx_fee(self, vin_infos, total_out_value):
        """
        计算交易手续费
        fee = total_in_value - total_out_value
        """
        total_in_value = 0
        for tx_infos in seperate_big_list(vin_infos, chunk=self.request_chunk_size):
            tx_ids = (tx['txid'] for tx in tx_infos)
            tx_index = (tx['index'] for tx in tx_infos)
            tx_details = self.rpc.get_transactions(tx_ids)
            if not tx_details:
                continue
            for tx_detail, index in zip(tx_details, tx_index):
                v_out_list = tx_detail.get('vout')
                if not v_out_list:
                    continue
                v_out = v_out_list[index]
                v_out_value = self.get_handle_value(v_out)
                total_in_value += v_out_value

        fee = total_in_value - total_out_value

        return fee

    def get_handle_value(self, v_out):
        """
        处理value
        """
        return int(Decimal(str(v_out.get("value", 0))) * Decimal(pow(10, self.coin_value_unit)))

    def get_vout_address(self, tx_type, v_out):
        """
        根据交易类型获取输出地址
        """
        if tx_type in self.invalid_tx_type_list:
            return ""
        if tx_type in self.segwit_tx_type_list:
            v_out_address = self.get_segwit_address_from_vout(v_out)
        else:
            v_out_address = self.get_address_from_vout(v_out)

        return v_out_address

    def get_address_from_vout(self, v_out):
        """
        从交易输出vout中解析输出地址
        """
        tx_out_oj = v_out.get("scriptPubKey")
        if not tx_out_oj:
            return None
        address = tx_out_oj.get("addresses")
        if not address:
            return None

        return address[0]

    def get_segwit_address_from_vout(self, v_out):
        """
        返回隔离验证交易的输出地址
        """
        return ""

    def get_transaction_type(self, v_out):
        """
        返回交易输出的类型
        """
        tx_out_oj = v_out.get("scriptPubKey")
        if not tx_out_oj:
            return None
        tx_type = tx_out_oj.get("type")

        return tx_type

    def parse_mempool_info(self, redis_mempool):
        """
        解析内存池中的交易数据,与redis中的数据比较,实现增量推送
        """
        node_mempool_set = set(self.rpc.get_getrawmempool())
        if not node_mempool_set:
            return [], [], []
        new_tx_ids = node_mempool_set.difference(set(redis_mempool))
        old_tx_ids = list(set(redis_mempool).difference(node_mempool_set))

        push_list = []

        for tx_ids in seperate_big_list(list(new_tx_ids), chunk=self.request_chunk_size):
            tx_details = self.rpc.get_transactions(tx_ids)
            if not tx_details:
                continue
            txs_list, _ = self.parse_normal_tx(tx_details, mempool_tx=True)
            push_list.extend(txs_list)

        return old_tx_ids, new_tx_ids, push_list

    def get_address_from_scriptsig(self, script_sig):
        """
        从解锁脚本中解析出地址,尚不支持隔离见证类型的解锁脚本(签名)
        """
        address = None
        try:
            script_sig_byte = bytes.fromhex(script_sig)
            if not script_sig_byte:
                return None
            n = script_sig_byte[0]
            if n == 0:
                address = self.gen_address_p2sh(script_sig_byte)
            else:
                address = self.gen_address_p2pk(script_sig_byte)
        except Exception:
            err_info = traceback.format_exc()
            G_LOGGER.error("{} GET_ADDRESS_FROM_SCRIPTSIG ERROR：{}".format(self.coin_type, err_info))

        return address

    def gen_address_p2pk(self, script_bytes):
        """
        根据p2pkh的签名信息解析出pubkey,pubkey--->address
        param: magicbyte 版本前缀
        """
        magicbyte, magicbyte_length = self.get_address_magicbyte(address_type='P2PKH')
        curosr = 0
        while curosr < len(script_bytes):
            n, length = self.get_curosr_vrant(script_bytes, curosr)
            curosr += n
            content = script_bytes[curosr: curosr + length]
            curosr += length

        address = self.pubkey_to_address(content, magicbyte=magicbyte, magicbyte_length=magicbyte_length)
        G_LOGGER.debug("{} ---> P2PKH Address: {}".format(self.coin_type, address))

        return address

    def gen_address_p2sh(self, script_bytes):
        """
        p2sh根据验证信息解析出reeddemscript, reeddemscript--->address
        param: magicbyte 版本前缀
        """
        magicbyte, magicbyte_length = self.get_address_magicbyte(address_type='P2SH')
        curosr = 1
        while curosr < len(script_bytes):
            n, length = self.get_curosr_vrant(script_bytes, curosr)
            curosr += n
            content = script_bytes[curosr: curosr + length]
            curosr += length

        address = self.p2sh_scriptaddr(content.hex(), magicbyte=magicbyte, magicbyte_length=magicbyte_length)
        G_LOGGER.debug("{} ---> P2PKH Address: {}".format(self.coin_type, address))

        return address

    def get_address_magicbyte(self, address_type='P2PKH'):
        """
        返回生成地址的版本前缀
        """
        magicbyte_length = 1
        address_type = address_type.upper()
        if address_type == 'P2PKH':
            magicbyte = 0
        elif address_type == 'P2SH':
            magicbyte = 5
        else:
            magicbyte = 0

        return magicbyte, magicbyte_length

    def get_curosr_vrant(self, script_bytes, curosr):
        """
        解析当前操作码的含义,置游标的位置,返回游标位移和需要入栈的字节数
        """
        opcode = script_bytes[curosr]
        if opcode < 76:
            n, length = 1, opcode
        elif opcode == 76:    # 0x4c
            n, length = 2, script_bytes[curosr + 1]
        elif opcode == 77:    # 0x4d
            n, length = 3, (Struct('<H').unpack_from(script_bytes, curosr + 1))[0]
        elif opcode == 78:    # 0x4e
            n, length = 5, (Struct('<I').unpack_from(script_bytes, curosr + 1))[0]
        else:
            n, length = 1, opcode

        return n, length

    def p2sh_scriptaddr(self, script, magicbyte=5, magicbyte_length=1):
        """
        根据reedem-script生成地址
        """
        if re.match('^[0-9a-fA-F]*$', script):
            script = bitcoin.binascii.unhexlify(script)
        return self.hex_to_b58check(bitcoin.hash160(script), magicbyte=magicbyte, magicbyte_length=magicbyte_length)

    def pubkey_to_address(self, pubkey, magicbyte=0, magicbyte_length=1):
        """
        根据pubkey生成地址
        """
        if isinstance(pubkey, (list, tuple)):
            pubkey = bitcoin.encode_pubkey(pubkey, 'bin')

        if len(pubkey) in [66, 130]:
            return self.bin_to_b58check(bitcoin.bin_hash160(bitcoin.binascii.unhexlify(pubkey)), magicbyte=magicbyte, magicbyte_length=magicbyte_length)

        return self.bin_to_b58check(bitcoin.bin_hash160(pubkey), magicbyte=magicbyte, magicbyte_length=magicbyte_length)

    def hex_to_b58check(self, inp, magicbyte=0, magicbyte_length=1):
        inp = bytes.fromhex(inp)
        return self.bin_to_b58check(inp, magicbyte=magicbyte, magicbyte_length=magicbyte_length)

    def bin_to_b58check(self, inp, magicbyte=0, magicbyte_length=1):
        inp_fmtd = int(magicbyte).to_bytes(magicbyte_length, 'big') + inp
        checksum = bitcoin.bin_dbl_sha256(inp_fmtd)[:4]

        leadingzbytes = 0
        for x in inp_fmtd:
            if x != 0:
                break
            leadingzbytes += 1

        return '1' * leadingzbytes + bitcoin.changebase(inp_fmtd + checksum, 256, 58)


class BtcTokenParser(BtcParserBase):
    def __init__(self):
        super().__init__()
        self.hex_len = 40
        self.op_id = None
        self.enable_op_id = False
        # 每次向服务器请求tx的数量
        self.transaction_pos = 50
        self.TX_TYPE = 'nulldata'
        self.OP_RETURN = "OP_RETURN"
        self.script_pos = 4    # script hex前面不需要的数据量
        self.token_name_len = 16
        self.token_opid_len = 8
        self.token_value_len = 16
        self.printable = string.printable[:-6]
        self.wei = 8

    def split_token(self, script_hex) -> tuple:
        """
        :param script_hex:
        :return: 返回的信息tuple中任意元素为None, 则表示解析不正确
        """
        token_name, token_opid, token_value = None, None, None
        token_name_hex = script_hex[:self.token_name_len]
        token_opid_hex = script_hex[self.token_name_len:self.token_name_len + self.token_opid_len]
        token_value_hex = script_hex[self.token_name_len + self.token_opid_len: self.token_name_len + self.token_opid_len + self.token_value_len]

        if token_name_hex:
            token_name = "".join([i for i in bytes.fromhex(token_name_hex).decode() if i in self.printable])

        if token_opid_hex:
            token_opid = int(token_opid_hex, 16)

        if token_value_hex:
            token_value = str(int(token_value_hex, 16))

        return token_name, token_opid, token_value

    def verify_op_id(self, op_id):
        """
        资产开启, 若enable_op_id为False状态, 则表示不校验op_id是否正确, 直接通过
        否则仅当op_id相等时才通过, 否则不通过.
        """
        if not self.enable_op_id:
            return True
        if op_id == self.op_id:
            return True
        return False

    def parse_txs(self, txs, height=None):
        """
        解析区块内交易
        """
        push_list = []
        # 增加回溯缓存逻辑
        push_cache = []
        if self.rollback and height:
            cache_tag = self.push_cache.get(str(height))
            push_cache = self.push_cache.pop(str(height)) if cache_tag else []

        start = 0
        while start <= len(txs):
            # 直接通过USDT节点获取交易
            push_list.extend(self.parse_normal_tx(txs[start:start + self.transaction_pos], push_cache=push_cache))
            start += self.transaction_pos

        return push_list

    def get_transaction_value(self, v_out: dict) -> str or False:
        tx_token_info = self.get_transaction_return_info(v_out)
        if tx_token_info:
            return tx_token_info[2]
        return False

    def get_transaction_return_info(self, v_out: dict) -> str or False:
        tx_out_oj = v_out.get("scriptPubKey")
        if not tx_out_oj:
            return False
        script_asm, script_hex = tx_out_oj.get('asm', False), tx_out_oj.get('hex', "")
        if script_hex:
            script_hex = script_hex[self.script_pos:]
        if (not script_asm) or (not script_asm.startswith(self.OP_RETURN)) or (len(script_hex) != self.hex_len):
            return False
        token_info = self.split_token(script_hex)
        if None in token_info:
            return False
        return token_info

    def find_op_return(self, v_outs):
        for v_out in v_outs:
            tx_token_info = self.get_transaction_return_info(v_out)
            if tx_token_info:
                return tx_token_info

    def parse_mempool_info(self, redis_mempool):
        """
        解析内存池中的交易数据,与redis中的数据比较,实现增量推送
        """
        node_mempool_set = set(self.rpc.get_getrawmempool())
        if not node_mempool_set:
            return [], [], []
        new_tx_ids = node_mempool_set.difference(set(redis_mempool))
        old_tx_ids = list(set(redis_mempool).difference(node_mempool_set))

        new_tx_ids_list = list(new_tx_ids)

        push_list = []
        start = 0
        while start <= len(new_tx_ids_list):
            # 直接通过USDT节点获取交易
            push_list.extend(self.parse_normal_tx(new_tx_ids_list[start:start + self.transaction_pos], mempool_tx=True))
            start += self.transaction_pos
        return old_tx_ids, new_tx_ids, push_list

    def real_number(self, number, places=None, *, eng_string=False):
        if places:
            decimals = Decimal(number) * Decimal(pow(10, places))
        else:
            decimals = Decimal(number)

        if eng_string:
            return decimals.to_integral().to_eng_string()
        return decimals.to_integral()

    def real_to_str(self, number: Decimal):
        return number.to_eng_string()

    def parse_normal_tx(self, tx_ids, mempool_tx=False, push_cache=None):
        """
        解析交易通过USDT节点
        """
        push_list = []
        tx_details = self.rpc.omni_get_transactions(tx_ids)

        for idx, tx_detail in enumerate(tx_details):
            if tx_detail is None:
                # 不过USDT交易
                continue
            mq_tx = G_PUSH_TEMPLATE.copy()
            mq_tx["Txid"] = tx_ids[idx]
            if self.rollback and push_cache and (mq_tx["Txid"] in push_cache):
                continue
            mq_tx["Type"] = self.coin_type
            mq_tx['Valid'] = tx_detail.get('valid', False)
            mq_tx['status'] = str(tx_detail.get('valid', "false")).lower()
            mq_tx['Fee'] = self.real_number(tx_detail.get("fee", "0"), self.wei, eng_string=True)

            if mempool_tx:
                mq_tx["Time"] = int(time.time())
                mq_tx["BlockNumber"] = 0
            else:
                mq_tx["Time"] = int(tx_detail.get("blocktime", time.time()))
                mq_tx["BlockNumber"] = self.current_height

            mq_tx['From'] = send_addr = tx_detail.get('sendingaddress')
            mq_tx['To'] = to_addr = tx_detail.get('referenceaddress')
            if send_addr is None or to_addr is None:
                continue

            subsends = tx_detail.get('subsends')
            if not subsends:
                mq_tx['Amount'] = self.real_number(tx_detail.get('amount', "0"), self.wei, eng_string=True)
                op_id = tx_detail.get('propertyid')
                if self.verify_op_id(op_id):
                    mq_tx['Contract'] = str(op_id)
                else:
                    continue
                push_list.append(mq_tx)
            else:
                for out_idx, sub in enumerate(subsends):
                    if not isinstance(sub, dict):
                        continue
                    mq_tx['Amount'] = sub.get('amount', "0")
                    op_id = sub.get('propertyid')
                    if self.verify_op_id(op_id):
                        mq_tx['Contract'] = str(op_id)
                    else:
                        continue
                    push_list.append(mq_tx)

        return push_list
