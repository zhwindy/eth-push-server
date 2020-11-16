#!/usr/bin/env python
from lib.iplugin import TxPlugin
from core.globals import G_PUSH_TEMPLATE
from coin.btc.base import BtcParserBase

__all__ = ["Bch"]


class BchParser(BtcParserBase):
    """
    bch解析处理器
    修改为将所有地址改为BCH旧地址
    """
    def __init__(self):
        super().__init__()
        self.coin_type = "BCH"
        self.rollback = False
        self.rollback_count = 0
        from lib.bch_convert.convert import to_legacy_address
        self.to_legacy_address = to_legacy_address

    def parse_coinbase_tx(self, tx_id, mempool_tx=False, push_cache=None):
        if tx_id == '4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b':
            return []
        return super().parse_coinbase_tx(tx_id, mempool_tx=mempool_tx, push_cache=push_cache)

    def get_address_from_vout(self, v_out):
        """
        从交易输出vout中解析种输出地址
        """
        origin_address = v_out["scriptPubKey"]["addresses"][0]
        # address = origin_address.split(":")[-1]

        return origin_address

    def get_vout_address(self, tx_type, v_out):
        v_out_address = super().get_vout_address(tx_type, v_out)
        if not v_out_address:
            return v_out_address
        try:
            old_addr = self.to_legacy_address(v_out_address)
        except Exception:
            old_addr = v_out_address

        return old_addr

    # def get_address_magicbyte(self, address_type='P2PKH'):
    #     """
    #     返回生成地址的版本前缀
    #     """
    #     magicbyte_length = 1
    #     address_type = address_type.upper()
    #     if address_type == 'P2PKH':
    #         magicbyte = 28
    #     elif address_type == 'P2SH':
    #         magicbyte = 40
    #     else:
    #         magicbyte = 0
    #
    #     return magicbyte, magicbyte_length


class Bch(TxPlugin):

    name = "bch"
    desc = "解析bch数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = BchParser()

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        self.parser.rollback = rollback
        self.parser.rollback_count = rollback_count
        return self.parser.parse_block(block_num)

    def push_mempool_list(self, redis_mempool):
        return self.parser.parse_mempool_info(redis_mempool)
