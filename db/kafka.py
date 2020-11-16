import json
from queue import Queue
import sys
import time
from core.globals import G_LOGGER, G_CFG
from kafka import KafkaProducer


class AsyncError(Exception):
    def __init__(self, code, msg):
        self.code = code
        if msg is None:
            self.msg = code
        else:
            self.msg = msg


class KafkaError(AsyncError):
    pass


class KafkaQueue():

    def __init__(self, cfg):
        self.cfg = cfg
        self.logger = G_LOGGER
        mode = self.cfg.coin.coin_dict['mode']
        if mode == 'dev':
            # 本地测试时，使用本地的kafka地址
            self.producer = KafkaProducer(
                bootstrap_servers=['127.0.0.1:9092'],
                api_version=(0, 10),
                retries=5
            )
        else:
            # kafka的broker目前没有外网，只能在内网服务器上访问
            self.producer = KafkaProducer(
                bootstrap_servers=[
                    '172.17.163.50:9092',
                    '172.17.163.49:9092',
                    '172.17.163.48:9092'
                ],
                api_version=(0, 10),
                retries=5
            )

    def send(self, data):
        msg = json.dumps(data)
        coin = self.cfg.coin.coin_dict['name']
        topic_name = 'coldlar_{}'.format(coin)
        # print('topic_name={}'.format(topic_name))
        future = self.producer.send(topic_name, msg.encode())
        res = future.get()
        return res.partition, res.offset

    def __del__(self):
        self.producer.close()


if __name__ == '__main__':
    kafka = KafkaQueue(G_CFG)
    data = {
        "Txid": "C57297DAD3FEF1F72F89A399EC3C4E8F3BC17E10E449E24AAF1BAE266E0F9508",
        "Type": "BNB",
        "From": "bnb1jxfh2g85q3v0tdq56fnevx6xcxtcnhtsmcu64m",
        "To": "bnb1wdhhmdhsjdhsq5pmk4n3dyd9x2rp0jk8cey3fp",
        "Amount": "0.09900000",
        "Time": 1574670354,
        "BlockNumber": 50799653,
        "Contract": "",
        "Charge": False,
        "Memo": "",
        "Fee": 0.00037500,
        "Action": "",
        "Valid": True,
        "VoutsIndex": 0,
        "TxIndex": 0
    }
    kafka.send(data)
