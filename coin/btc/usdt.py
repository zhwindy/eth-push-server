
from coin.btc.base import BtcTokenParser
from lib.iplugin import TxPlugin

__all__ = ["Usdt"]


class UsdtParser(BtcTokenParser):
    def __init__(self):
        super().__init__()
        self.coin_type = "BTC"
        self.op_id = 31
        self.enable_op_id = True
        self.rollback = False
        self.rollback_count = 0


class Usdt(TxPlugin):
    name = "usdt"
    desc = "解析usdt交易数据"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self.parser = UsdtParser()
        self.op_id = None
        self.enable_op_id = True

    def newest_height(self):
        return self.parser.newest_height()

    def push_list(self, block_num, rollback=False, rollback_count=0):
        self.parser.rollback = rollback
        self.parser.rollback_count = rollback_count
        return self.parser.parse_block(block_num)

    def push_mempool_list(self, redis_mempool):
        return self.parser.parse_mempool_info(redis_mempool)


if __name__ == "__main__":
    u = Usdt()
    r = u.push_list(596812)
    import json
    print(json.dumps(r))
