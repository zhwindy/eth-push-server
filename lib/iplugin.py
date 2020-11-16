#!/usr/bin/env python


class TxPlugin:
    """ 交易数据解析模板 """
    name = ''
    desc = ''
    version = ''

    def newest_height(self):
        """
        获取最新高度接入点函数
        return:
        """
        raise NotImplementedError('must implement newest_height function')

    def push_list(self, block_num):
        """
        获取推送数据接入点函数
        :return:
        """
        raise NotImplementedError('must implement parse function')
