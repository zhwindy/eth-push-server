#!/usr/bin/env python
from lib.log import Log
from settings import CFG


G_CFG = CFG()
G_LOGGER = Log(G_CFG)

# 通用普通交易推送模板
G_PUSH_TEMPLATE = {
    "Txid": "",
    "Type": "",
    "From": "",
    "To": "",
    "Amount": "",
    "Time": "",
    "BlockNumber": 0,
    "Contract": "",
    "Charge": False,    # 是否为找零
    "Memo": "",     # 交易信息
    "Fee": 0,    # 手续费
    "Action": "",  # 交易类型
    "Valid": "",
    "VoutsIndex": 0,
    "status": "unconfirm",  # 交易是否生效
}


# 以太坊多签名交易推送模板
G_ETH_MULTI_PUSH_TEMPLATE = {
    "operationId": "",
    "Txid": "",
    "owners": "",
    "isPayer": "",
    "from": "",
    "to": "",
    "multContract": "",
    "signN": "",
    "maxN": "",
    "height": "",
    "time": "",
    "value": "",
    "confirmed": "",
    "codeType": "",
    "contract": "",
    "Fee": 0,    # 手续费
    "Valid": "",  # 交易状态
    "status": "unconfirm",  # 交易是否生效
}
