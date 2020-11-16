import json
from queue import Queue
import sys
import time
import pika
from pika import exceptions as pika_exceptions
from core.globals import G_LOGGER, G_CFG

if sys.version_info[0] == 3:
    xrange = range


class AsyncError(Exception):
    def __init__(self, code, msg):
        self.code = code
        if msg is None:
            self.msg = code
        else:
            self.msg = msg


class MQError(AsyncError):
    pass


class RabbitQueue():
    """MQ对象对象, 开始时"""
    """使用了队列, 所以是线程安全的."""
    # channel pool
    _pools = {}

    def __init_config(self):
        try:
            assert self.cfg.mq
        except AssertionError:
            self.logger.error('配置文件中不存在MQ配置, 无法初始化.')
            raise MQError(-1, "配置文件中不存在MQ配置, 无法初始化.")
        self.queue_config = dict()
        self.queue_config[self.cfg.mq.mq_dict['vhost']] = self.cfg.mq.mq_dict
        self.retry_times = 3

    def __init__(self, cfg, size=1):
        self.cfg = cfg
        self.logger = G_LOGGER
        self.__init_config()
        for k, cfg in self.queue_config.items():
            self._pools[k] = Queue(size)
            for i in xrange(size):
                self.create_rab_connection(k=k, username=cfg.get('username'), password=cfg.get('password'),
                                           host=cfg.get('host'), port=cfg.get('port'), vhost=cfg.get('vhost'))

    def create_rab_connection(self, username, password, host, port, vhost, k, is_requeue: bool = True):
        credentials = pika.PlainCredentials(username=username, password=password)
        rab_connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port, virtual_host=vhost, credentials=credentials))
        channel = self.get_channel(rab_connection)
        not is_requeue or self._pools[k].put(channel)
        return channel

    def get_obj(self, v_host):
        return self._pools[v_host].get()

    def return_obj(self, v_host, obj):
        self._pools[v_host].put(obj)

    def get_channel(self, obj):
        # obj = self.get_obj(v_host)
        return obj.channel()

    def send_msg(self, e, key, msg, channel=None):
        channel.basic_publish(exchange=e, routing_key=key, body=msg)  # ResourceError

    def put_to_que(self, e, key, v_host, msg):
        """入队, 如果channel已经被关闭, 生成新的channel使用. channel保持常开"""
        count = 0  # max try 3 times
        while True:
            obj, channel = None, None
            cfg = self.queue_config[v_host]
            try:  # maybe error ChannelClosed, ConnectionClosed and NoFreeChannels and NotBoundError
                channel = self.get_obj(v_host)
                self.send_msg(e, key, msg, channel)
            except pika_exceptions.ChannelClosed:
                channel = self.create_rab_connection(username=cfg.get('username'),
                                                     password=cfg.get('password'),
                                                     host=cfg.get('host'),
                                                     port=cfg.get('port'),
                                                     vhost=cfg.get('vhost'),
                                                     k=v_host,
                                                     is_requeue=False)
                self.send_msg(e, key, msg, channel)
            except pika_exceptions.ConnectionClosed:  # close
                # reconnection and send msg
                channel = self.create_rab_connection(username=cfg.get('username'),
                                                     password=cfg.get('password'),
                                                     host=cfg.get('host'),
                                                     port=cfg.get('port'),
                                                     vhost=cfg.get('vhost'),
                                                     k=v_host,
                                                     is_requeue=False)
                self.send_msg(e, key, msg, channel)
            except pika_exceptions.NoFreeChannels as err:  # Not session or max
                if count < self.retry_times:
                    count += 1
                    time.sleep(2)
                    continue
                else:
                    self.logger.warn('MQ Not Free Channels {}'.format(err))
                    break
            except Exception as err:
                self.logger.error('向MQ中插入信息遇到不常见异常,  {}'.format(err))
                return False
            finally:
                if channel is not None:
                    self.return_obj(v_host, channel)
            return True

    def rab_post_message(self, json_in, *, e_name, v_host, routing_key, typ="topic", is_durable=True):
        """True 成功 False 失败"""

        json_in_message = json.dumps(json_in, ensure_ascii=False)
        is_put = self.put_to_que(e_name, routing_key, v_host, json_in_message)
        return is_put

    def close(self):
        for k in self.queue_config.keys():
            while not self._pools[k].empty():
                try:
                    self.get_obj(k).close()
                except Exception:
                    pass

    def __del__(self):
        self.close()


if __name__ == '__main__':
    mq = RabbitQueue(G_CFG)
    mq.rab_post_message({"operationId": "0x88bfe66db2f35a8dc402c0f6e08a86ead86372bce8ea0b8d6cdb15d15e769f20", "Txid": "0xa28730ff0105328a193f59d58142db917ae0ed78bddf15b9280bb53a5cd41c00", "owners": ["0xe012d7edd31a4f1da56cee050e0faff9ab27b070", "0xc86867ac71ad7e625bee765c4d1495f51c42be70", "0xa61f245711a0e28f3525396a458a239bab445029"], "isPayer": 1, "from": "0xe012d7edd31a4f1da56cee050e0faff9ab27b070", "to": "", "multContract": "0x04fc327f1e466838a055ce6d99774a5bf496dc01", "signN": 3, "maxN": 2, "height": 6877709, "time": "0x5c12087e", "value": "0x2386f26fc10000", "confirmed": 0, "codeType": "001", "contract": ""}, e_name='eth.transaction.multsign', routing_key="*",
                        v_host='blockchain')