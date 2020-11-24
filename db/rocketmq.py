#!/usr/bin/env python
# coding=utf8
import sys
import json
import time
from mq_http_sdk.mq_exception import MQExceptionBase
from mq_http_sdk.mq_producer import TopicMessage
from mq_http_sdk.mq_client import MQClient


class RocketMQ(object):

    ACCESS_KEY = ""
    SECRET_KEY = ""

    def __init__(self, mode):
        self.mode = mode
        if mode == "dev":
            HTTP_ENDPOINT = ""
            TOPIC_NAME = "Test"
            INSTANCE_ID = ""
        else:
            HTTP_ENDPOINT = ""
            TOPIC_NAME = ""
            INSTANCE_ID = ""

        self.mq_client = MQClient(HTTP_ENDPOINT, self.ACCESS_KEY, self.SECRET_KEY)
        self.producer = self.mq_client.get_producer(INSTANCE_ID, TOPIC_NAME)

    def push_datas(self, data_list):
        """
        批量-推入数据
        """
        try:
            for data in data_list:
                msg = TopicMessage(json.dumps(data), "eth-tx")
                re_msg = self.producer.publish_message(msg)
                # print("Publish Message Succeed. MessageID:%s, BodyMD5:%s" % (re_msg.message_id, re_msg.message_body_md5))
        except MQExceptionBase as e:
            if e.type == "TopicNotExist":
                # print("Topic not exist, please create it.")
                sys.exit(1)
            # print("Publish Message Fail. Exception:%s" % e)
        return re_msg.message_id

    def push_data(self, data):
        """
        单个-推入数据
        """
        msg = TopicMessage(json.dumps(data), "eth-tx")
        re_msg = self.producer.publish_message(msg)
        # print("Publish Message Succeed. MessageID:%s, BodyMD5:%s" % (re_msg.message_id, re_msg.message_body_md5))
        return re_msg.message_id



if __name__ == "__main__":
    msg_list = [
        {'Txid': '0x04be830c078f101220a51f8a6a1db6895c204fe7111f0a63e966c418dfa2b8c4', 'Type': 'ETH', 'From': '0x937626359acb58665df1006f1912a12b2281caed', 'To': '0x937626359acb58665df1006f1912a12b2281caed', 'Amount': 0, 'Time': 1605482178000, 'BlockNumber': 11265386, 'Contract': '', 'Charge': False, 'Memo': '', 'Fee': 277200000000000, 'Action': '', 'Valid': True, 'VoutsIndex': 0, 'status': 'true'},
        {'Txid': '0xae10a191dc4cdd910b5449cbd1fb5001fa89f2d4a06565ad94b16b2987e05ba1', 'Type': 'ETH', 'From': '0x77e20f0ee4315b38025610cca1152d6eefcca59e', 'To': '0x4b898c5091f894e918841456e341a146e1f12662', 'Amount': 1680000000, 'Time': 1605482178000, 'BlockNumber': 11265386, 'Contract': '0xdac17f958d2ee523a2206206994597c13d831ec7', 'Charge': False, 'Memo': '', 'Fee': 354190775160000, 'Action': '', 'Valid': True, 'VoutsIndex': 0, 'status': 'true'}
    ]
    mode = 'dev'
    rocket = RocketMQ(mode)
    rocket.push_data(msg_list)
