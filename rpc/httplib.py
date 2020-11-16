#!/usr/bin/env python

import time
import json
import traceback
import requests

from urllib import parse
from core.globals import G_LOGGER
from requests import exceptions as request_error
from lib.error import UrlError, PayloadError, RequestError


class HttpMixin:

    def __init__(self, timeout=(60, 60)):
        self.timeout = timeout   # 默认60秒超时
        self.auth = None
        self.is_json = False
        self.headers = {}
        self.session = requests.Session()

    @classmethod
    def split_netloc(cls, netloc):
        split = netloc.split('@')
        if len(split) == 2:
            url_auth, url_net = split[0], split[1]
        elif len(split) == 1:
            url_auth, url_net = None, split[0]
        else:
            url_auth, url_net = '@'.join(split[:-1]), split[-1]
        return url_auth, url_net

    @classmethod
    def split_auth(cls, url_auth):
        auth_split = url_auth.split(':')
        username, password = auth_split[0], '.'.join(auth_split[1:])
        return username, password

    @classmethod
    def split_address(cls, address):
        host, port = address.split(':')
        return host, port

    @staticmethod
    def url_check(url_str):
        schemes = ['http', 'https']
        url = parse.urlparse(url_str)
        url_auth, url_net = HttpMixin.split_netloc(url.netloc)
        if url.scheme not in schemes:
            raise UrlError("URL必需以http或https开头.")
        if not url_net:
            raise UrlError("缺少必要的URL")

        host, port = HttpMixin.split_address(url_net)
        if not host:
            raise UrlError('URL中的host不正确')
        if port:
            try:
                port_int = int(port)
            except Exception:
                raise UrlError('URL中端口必须为数字')
            if not ((port_int <= 65535) and (port_int >= 1)):
                raise UrlError('URL端口范围必须在1 - 65535之间')
        return True

    @classmethod
    def get_url_parses(cls, uri):
        url_auth, url_netloc = cls.split_netloc(uri)
        username, password = cls.split_auth(url_auth)
        host, port = cls.split_address(url_netloc)
        return username, password, host, port

    def set_auth(self, username, password):
        self.auth = (username, password)

    def set_json(self):
        self.headers.setdefault('content-type', 'application/json')
        self.is_json = True

    def set_headers(self, key, value):
        self.headers.setdefault(key.lower(), value.lower())

    def _send_data(self, method, url, _params=None, _data=None, _json=None, _cookies=None):
        data = _params or _data or _json
        G_LOGGER.debug('接口: {}, 方法: {}, 请求数据: {}'.format(url, method, data))
        t1 = time.time()
        retry_time = 0.001
        while True:
            try:
                with self.session.request(method, url, params=_params, data=_data, json=_json, auth=self.auth,
                                          headers=self.headers, timeout=self.timeout, cookies=_cookies) as rsp:
                    G_LOGGER.debug('返回数据: {}'.format(rsp.text))
                    if 300 > rsp.status_code >= 200:
                        result = rsp.text
                        if self.is_json:
                            try:
                                t2 = time.time()
                                net_time = int((t2 - t1) * 1000) / 1000
                                res = json.loads(rsp.text)
                                t3 = time.time()
                                json_time = int((t3 - t2) * 1000) / 1000
                                all_time = int((t3 - t1) * 1000) / 1000
                                # G_LOGGER.info("json success, {}, {}, length={}, net_time={}, json_time={}, all_time={}, ".format(url, data, len(rsp.text), net_time, json_time, all_time))
                                return res
                            except Exception:
                                # G_LOGGER.info("json fail, {}, {}, length={}, net_time={}".format(url, data, len(rsp.text), net_time))
                                pass
                        return result
                    elif rsp.status_code == 404:
                        G_LOGGER.warn("[404] URL不存在, 请检查URL".format(self.auth))
                        raise UrlError("[404] URL不存在, 请检查URL")
                    elif rsp.status_code == 401:
                        G_LOGGER.warn("[401] auth验证错误. auth: {}".format(self.auth))
                        raise RequestError(rsp.status_code, "[401] auth验证错误. auth: {}".format(self.auth))
                    elif rsp.status_code == 403:
                        G_LOGGER.warn("[403] 没有权限操作此URL. url: {}".format(url))
                        raise RequestError(rsp.status_code, "[403] 没有权限操作此URL. url: {}".format(url))
                    elif 500 > rsp.status_code >= 400:
                        G_LOGGER.warn("[{}] 客户端请求错误. 错误详情: {}".format(rsp.status_code, rsp.text))
                        raise RequestError(rsp.status_code,
                                           "[{}] 客户端请求错误. 错误详情: {}".format(rsp.status_code, rsp.text),
                                           rsp)
                    elif 599 > rsp.status_code >= 500:
                        G_LOGGER.warn("[{}] 服务器响应错误. 错误文本: {}".format(rsp.status_code, rsp.text))
                        raise RequestError(rsp.status_code,
                                           "[{}] 服务器响应错误. 错误文本: {}".format(rsp.status_code, rsp.text),
                                           rsp)
            except (UrlError, RequestError):
                raise
            except (request_error.Timeout, request_error.ConnectionError) as e:
                if retry_time < 10:
                    retry_time = min(max(retry_time, retry_time * 2), 10)
                G_LOGGER.info("{}超时或被拒绝, retry_time={}, {} {}, 错误:{}".format(url, retry_time, method, data, str(e)))
                continue
            except request_error.InvalidURL:
                G_LOGGER.error('请求URL无效, 请检查: {}'.format(url))
                raise UrlError('URL无效.')
            except Exception as e:
                G_LOGGER.error('请求出现不可预知异常, 请处理. \r\n'
                                  '请求url: {}\r\n'
                                  '请求方法: {}\r\n'
                                  '请求参数: params: {}, data: {}, json: {}\r\n'
                                  '验证用户: {}\r\n'
                                  '错误详情: {}'.format(url, method, _params, _data, _json, self.auth,
                                                    traceback.format_exc()))
                raise RequestError(0, "请求出现不可预知异常, 请处理. 详情简要: {}".format(e))

    def format_params(self, params):
        _data, _json = None, None
        if self.is_json:
            if not isinstance(params, (dict, list)):
                try:
                    _json = json.loads(params)
                except Exception:
                    _data = params
            else:
                _json = params
        else:
            if isinstance(params, (dict, list)):
                try:
                    _data = json.dumps(params)
                except Exception as e:
                    raise PayloadError("参数无法转换为data请求数据, 请确认数据: {}".format(params))
            else:
                _data = params
        return _data, _json

    def _post(self, url, params):
        _data, _json = self.format_params(params)
        return self._send_data('post', url, _data=_data, _json=_json)

    def _get(self, url, params):
        return self._send_data("get", url, _params=params)

    def _single_post(self, url, params=None):
        return self._post(url, params)

    def _single_get(self, url, params=None):
        return self._get(url, params)


if __name__ == '__main__':
    from settings import CFG

    cfg = CFG("config.ini")
    h = HttpMixin(cfg)
    url = 'http://coldlar:coldwalllet@btc1.coldlar.cc:20168'
    HttpMixin.url_check(url)
    h.set_auth("coldlar", "coldwallet")
    rsp_json = h._post(url, {'id': 1, "method": "getinfo", "params": []})
    print(rsp_json)


