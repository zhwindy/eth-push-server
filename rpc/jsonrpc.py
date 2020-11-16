#!/usr/bin/env python

import json
from core.globals import G_LOGGER
from rpc.httplib import HttpMixin
from lib.error import JsonRpcError, RequestError, UrlError


class JsonRpcV1:
    def __new__(cls, *args, **kwargs):
        raise NotImplementedError


class JsonRpcV2:
    def __new__(cls, *args, **kwargs):
        raise NotImplementedError


class JsonRpcCompatibility:
    def __new__(cls, *args, **kwargs):
        raise NotImplementedError


class JsonRpcRequest(HttpMixin):
    def __init__(self, cfg):
        self.cfg = cfg
        timeout = cfg.rpc.rpc_dict.get("timeout")
        super().__init__(timeout)
        self.url = cfg.rpc.rpc_dict["url"]
        self.url_check(self.url)
        # if cfg.rpc.username and cfg.rpc.password:
        #     self.set_auth(cfg.rpc.username, cfg.rpc.password)
        self.set_json()
        self.id = 0

    def get_id(self):
        id = self.id
        self.id += 1
        yield id

    def reset_id(self):
        self.id = 0

    def _post(self, params, processor):
        try:
            result = super()._post(self.url, params=params)
        except UrlError as e:
            # 因为上面已经检测过url了, 所以这里不存在404的情况, 只会出现method不正确
            result = {'error': {"code": -32601, "message": "Method Not Found"}}
        except RequestError as e:
            if e.code >= 500:
                result = {'error': {"code": -32603, "message": "内部错误. 错误详情: {}".format(e.obj.text)}}
            elif e.code == 0:
                result = {'error': {"code": 0, "message": "不可预知的错误"}}
            else:
                result = {'error': {'code': -30600, "message": "无效的请求"}}
        result = processor(result)
        self.reset_id()
        return result

    def _get(self, params, processor):
        """JSONRPC不支持get请求, 如果使用, 将强转为post请求, 并发出一条警告."""
        G_LOGGER.warn('JsonRpc不支持GET请求, 自动转为post请求, 建议所有使用get请求链接转为post请求!')
        return self._post(params=params, processor=processor)

    @classmethod
    def right_params(cls, p):
        if isinstance(p, (tuple, list)):
            return p
        if isinstance(p, (set,)):
            return list(p)
        if isinstance(p, (int, float, str, dict)):
            return [p]
        if p is None:
            return []
        return [p]

    def _single_post(self, method, params=None):
        """强制使用jsonrpc 2.0版本"""
        def processor(data):
            # 部分jsonrpc返回的数据包含了特殊字符导致不是dict而是str类型
            # 临时增加str转dict的代码(非严格转换)
            if isinstance(data, str):
                data = json.loads(data, strict=False)
            err = data.get('error')
            if not err:
                return data['result']
            raise JsonRpcError(code=err.get('code', -32603), msg=err.get('message', ''))

        payload = {"jsonrpc": "2.0", "id": next(self.get_id()), "method": method,
                   "params": self.right_params(params)}
        return self._post(payload, processor)

    def _many_post(self, method, params, ignore_err=True):

        def processor(data):
            """JSONRPC协议中, result与error互斥, 两者不可能同时拥有值."""
            results = []
            for d in data:
                if not isinstance(d, dict):
                    continue
                results.append(d.get('result'))
            if ignore_err:
                return results
            if not all(results):
                G_LOGGER.debug('many post中返回数据错误: {}'.format(data))
                raise JsonRpcError(-1, "many post中返回数据中错误.")

        payload = [{'jsonrpc': "2.0", "id": next(self.get_id()),
                    'method': method, "params": self.right_params(p)} for p in params]
        return self._post(payload, processor)

    def _get_func(self, arg):
        if arg:
            return self._many_post
        if isinstance(arg, (tuple, list, set, range)):
            return self._many_post
        return self._single_post

    def _get_params(self, ps, *args):
        if isinstance(ps, (list, tuple, set, range)):
            params = [[p, *args] for p in ps]
        else:
            params = [ps, *args]
        return params


if __name__ == '__main__':
    from settings import CFG

    cfg = CFG("../dsc.conf")
    # b = BitcoinRpc(cfg)
    # info = b.get_info()
    # print(info)
    # info2 = b.get_block_chain_info()
    # print(info2)
    # block_hash = b.get_block_hash(1, 10)
    # print(block_hash)
    # block = b.get_block(block_hash)
    # print(block)
    # b.get_mempool_ancestors('0000000000000000007b8b6081605aed881d983c97d91a47d64897d7fa22f46e')
    # print(b.estimate_smart_fee(6))
    # e = EthereumRpc(cfg)
    # print(e.eth_syncing())
    # print(e.eth_block_number())
    # print(e.get_block_by_number(200000, 2))
    # print(e.get_block_by_hash(['0x13ced9eaa49a522d4e7dcf80a739a57dbf08f4ce5efc4edbac86a66d8010f693',
    #                            '0x8faf8b77fedb23eb4d591433ac3643be1764209efa52ac6386e10d1a127e4220']))

    # print(e.eth_call([{
    #   "from": "0x600575b3f587fC78c9954715cE663e1D4e0CDf23",
    #   "to": "0xe1ee8578fb0bd17824754d32f5fa26b6dac26b9f",
    #   "gas": "0xea60",
    #   "gasPrice": "0x4e3b29200",
    #   "value": "0x0",
    #   "data": "0x70a08231000000000000000000000000e1ee8578fb0bd17824754d32f5fa26b6dac26b9f"
    # }, {
    #   "from": "0x600575b3f587fC78c9954715cE663e1D4e0CDf23",
    #   "to": "0xe1ee8578fb0bd17824754d32f5fa26b6dac26b9f",
    #   "gas": "0xea60",
    #   "gasPrice": "0x4e3b29200",
    #   "value": "0x0",
    #   "data": "0x70a08231000000000000000000000000e1ee8578fb0bd17824754d32f5fa26b6dac26b9f"
    # }]))

    # print(e.eth_get_transaction_count(['0x600575b3f587fC78c9954715cE663e1D4e0CDf23', '0x600575b3f587fC78c9954715cE663e1D4e0CDf23']))
    # ps = ['0x0', '0x1', ['0x2', '0x3'], {"4": '0x4', "5": ['0x5', '0x6', {"7": '0x7'}]}, '8', "test"]
    # print(e._every_to_hex(ps))
    # print(ps)
    # import time
    # counter = 0
    # try:
    #     while True:
    #         counter += 1
    #         if counter % 50 == 0:
    #             print("计数器: {}".format(counter))
    #         result = e.eth_get_block_by_number(e.eth_block_number(), tx_detail=False)
    #         # print(result)
    #         time.sleep(1)
    # except Exception:
    #     print("计数器: {}".format(counter))
    # except KeyboardInterrupt:
    #     print("计数器: {}".format(counter))




