#!/usr/bin/env python
# coding=utf8
import sys
import json
import time
from mq_http_sdk.mq_exception import MQExceptionBase
from mq_http_sdk.mq_producer import TopicMessage
from mq_http_sdk.mq_client import MQClient

HTTP_ENDPOINT = "http://1219848471387692.mqrest.cn-qingdao-public.aliyuncs.com"
ACCESS_KEY = "LTAI4G7JnJiQfa6yqVBzSjH7"
SECRET_KEY = "Y7vWpIRTvVSwgfs8Svj91YNyrBm9gh"

# 初始化 client
mq_client = MQClient(HTTP_ENDPOINT, ACCESS_KEY, SECRET_KEY)
# 所属的 Topic
topic_name = "Test"
# Topic所属实例ID，默认实例为空None
instance_id = "MQ_INST_1219848471387692_BXVpFo7E"

producer = mq_client.get_producer(instance_id, topic_name)

msg_list = [
    {'Txid': '0x04be830c078f101220a51f8a6a1db6895c204fe7111f0a63e966c418dfa2b8c4', 'Type': 'ETH', 'From': '0x937626359acb58665df1006f1912a12b2281caed', 'To': '0x937626359acb58665df1006f1912a12b2281caed', 'Amount': 0, 'Time': 1605482178000, 'BlockNumber': 11265386, 'Contract': '', 'Charge': False, 'Memo': '', 'Fee': 277200000000000, 'Action': '', 'Valid': True, 'VoutsIndex': 0, 'status': 'true'},
    {'Txid': '0xae10a191dc4cdd910b5449cbd1fb5001fa89f2d4a06565ad94b16b2987e05ba1', 'Type': 'ETH', 'From': '0x77e20f0ee4315b38025610cca1152d6eefcca59e', 'To': '0x4b898c5091f894e918841456e341a146e1f12662', 'Amount': 1680000000, 'Time': 1605482178000, 'BlockNumber': 11265386, 'Contract': '0xdac17f958d2ee523a2206206994597c13d831ec7', 'Charge': False, 'Memo': '', 'Fee': 354190775160000, 'Action': '', 'Valid': True, 'VoutsIndex': 0, 'status': 'true'}
]
try:
    for i in msg_list:
        msg = TopicMessage(json.dumps(i), "eth-tx")
        # 设置属性
        # msg.put_property("a", "i")
        # 设置KEY
        # msg.set_message_key("MessageKey")
        re_msg = producer.publish_message(msg)
        print("Publish Message Succeed. MessageID:%s, BodyMD5:%s" % (re_msg.message_id, re_msg.message_body_md5))
        time.sleep(1)
except MQExceptionBase as e:
    if e.type == "TopicNotExist":
        print("Topic not exist, please create it.")
        sys.exit(1)
    print("Publish Message Fail. Exception:%s" % e)