import json
import redis
import time
from core.globals import G_LOGGER, G_CFG
from lib.tool import CachedProperty, Single, get_now


class RedisPool(Single):
    _pool = None

    def __init__(self, cfg):
        """
        host: ip host
        port: port must be digit
        db: redis database
        password: access redis password
        """
        self._pool = redis.ConnectionPool(**cfg.redis.redis_dict)

    def __call__(self, *args, **kwargs):
        return self._pool

    def __del__(self):
        self._pool.disconnect()

    def get_client(self):
        return redis.Redis(connection_pool=self._pool)


class RedisExtendMixin(object):
    def unzip_dict(self, **kwargs):
        return list(kwargs.keys()), list(kwargs.values())


class RedisDictMixin(object):
    # 如果需要操作dict, 继承这个类就OK了.
    def ltrim(self, key, start=0, stop=10000):
        """修剪列表"""
        self.client.ltrim(key, start, stop)

    def ltrim_asy(self):
        """异步修剪列表"""
        raise NotImplementedError

    def hash_set(self, platform, key, value, is_json=True):
        if is_json:
            try:
                value = json.dumps(value)
            except Exception:
                raise self.RedisError("set value is not JSON data")
        self.client.hset(platform, key, value)

    def hash_get(self, platform, key, is_json=True):
        result = self.client.hget(platform, key)
        if is_json:
            try:
                result = json.loads(result)
            except Exception:
                raise self.RedisError('get value is not JSON data')
        return result

    def hash_sets(self, platform, **kwargs):
        self.client.hmget(platform, self.unzip_dict(**kwargs))

    def join_hkey(self, coin, level, **kwargs):
        key = '{}_{}'.format(coin, level)
        unsure = '_'.join((kwargs.values()))
        key += unsure
        return key

    # def next_hkey(self, coin, level, **kwargs):
    #     return '{}_{}'.format(self.join_hkey(coin, level, **kwargs), self.handle.level[coin][level].next())

    def get_primary_key(self, platform, coin, level):
        key = '{}_{}'.format(platform, self.join_hkey(coin, level))
        return self.hash_get(platform, key, False)

    def set_primary_key(self, platform, coin, level, value):
        key = '{}_{}'.format(platform, self.join_hkey(coin, level))
        self.hash_set(platform, key, value, False)


class MempoolRedisSetMixin:
    """设置mempool中的string格式, 存放用户"""
    def sadd(self, address, *values):
        key = self.build_key(address)
        G_LOGGER.debug('key: {} 添加值: {}'.format(key, values))
        self.client.sadd(key, *values)

    def spop(self, address):
        # 这个一般不会用
        self.client.spop(self.build_key(address))

    def smembers(self, address):
        key = self.build_key(address)
        members = self.client.smembers(key)
        G_LOGGER.debug('查看用户所有元素 -- 用户: {} 添加值: {}'.format(key, members))
        return members

    def scard(self, address):
        key = self.build_key(address)
        count = self.client.scard(key)
        G_LOGGER.debug('查看用户元素数量 -- 用户: {} 数量: {}'.format(key, count))
        return count

    def srem(self, address, *values):
        # 删除一个地址的中tx_hash
        key = self.build_key(address)
        rm = self.client.srem(key, *values)
        G_LOGGER.debug('删除用户元素 -- 用户: {} 元素: {}'.format(key, values))
        return rm

    def sismember(self, address, value):
        key = self.build_key(address)
        is_mem = self.client.sismember(key, value)
        G_LOGGER.debug('是否为用户元素 -- 用户: {} 元素: {} 结果: {}'.format(key, value, is_mem))
        return is_mem


class MempoolRedisStringMixin:
    """设置mempool中的string格式, 存放交易"""
    def set(self, tx_hash, value):
        """设置数据"""

        key = self.build_key(tx_hash)
        value = json.dumps(value)
        G_LOGGER.debug("设置key: {}  值: {}".format(key, value))
        return self.client.set(key, value)

    def get(self, tx_hash):
        key = self.build_key(tx_hash)
        result = self.client.get(key)
        G_LOGGER.debug("获取key: {}  值为: {}".format(key, result))
        return result

    def get_and_delete(self, redis_key):
        key = self.build_key(redis_key)
        result = self.client.get(key)
        G_LOGGER.debug("获取key: {}  值为: {}".format(key, result))
        self.client.delete(key)
        return result


class RedisDB(RedisExtendMixin, MempoolRedisSetMixin, MempoolRedisStringMixin):
    class RedisError(Exception):
        pass

    def __init__(self, cfg):
        super().__init__()
        self.client = RedisPool(cfg).get_client()
        self.coin = cfg.coin.coin_dict["name"]
        # 推送成功的近1000条数据
        self.last_push_cache = {
            "cache_name": f"{self.coin}_last_push_cache",
            "cache_length": 1000
        }
        # 推送失败的数据
        self.fail_push_log = f"{self.coin}_fail_push_log"
        # basic
        self.basic_key = f"{self.coin}_basic"
        # mempool
        self.mempool_key = f"{self.coin}_mempool"
        # pending block
        self.pending_block_key = f"{self.coin}_pending_block"
        G_LOGGER.debug("与redis建立连接成功.")
        G_LOGGER.debug("币种: {} redis连接信息: {}".format(self.coin, cfg.redis.redis_dict))

    def build_key(self, key):
        key = isinstance(key, (str, )) and key or key.decode()
        return ''.join([self.coin, '_', key])

    def delete(self, *keys, has_key=False):
        if not keys:
            return
        if not has_key:
            keys = [self.build_key(isinstance(key, str) and key or key.decode()) for key in keys]
        else:
            keys = keys
        return self.client.delete(*keys)

    def get_keys(self, pattern='*'):
        """获得某个币种所有的keys, 用于删除所有key
        支持通过符, 可以定向删除
        *: 匹配任意字符0个或多个
        ?: 匹配任意字符0个或1个
        []: 包含
        地址: pattern="?" * 34
        tx_hash: pattern="?" * 64
        所有: pattern="*" 默认
        """
        keys = self.build_key(pattern)
        return self.client.keys(keys)

    @CachedProperty
    def push_cache(self):
        """获取推送成功的缓存"""
        self.client.ltrim(self.last_push_cache["cache_name"], 0, self.last_push_cache["cache_length"])
        return self.client.lrange(self.last_push_cache["cache_name"], 0, self.last_push_cache["cache_length"])

    def update_push_cache(self, item):
        """更新推送成功的缓存"""
        self.client.lpush(self.last_push_cache["cache_name"], item)
        self.client.ltrim(self.last_push_cache["cache_name"], 0, self.last_push_cache["cache_length"])

    def push_utxo_cache(self, utxo_cache_key, data):
        """
        保存xmr的utxo, 默认保存1000条
        """
        cache_len = self.client.llen(utxo_cache_key)
        if int(cache_len) >= 1000:
            self.client.ltrim(utxo_cache_key, 0, 1000)
        else:
            item = json.dumps(data)
            self.client.rpush(utxo_cache_key, item)

    def save_data_by_key(self, key, item):
        """根据key从右边保存数据"""
        self.client.rpush(key, item)

    def save_fail_push_log(self, item):
        """保存推送失败的日志"""
        self.save_data_by_key(self.fail_push_log, item)

    def save_pending_block(self, item):
        """保存待处理的块数据"""
        self.save_data_by_key(self.pending_block_key, item)

    def get_data_by_key(self, key):
        """根据key获取指定多少条数据"""
        value = self.client.lpop(key)
        return [value] if value else []

    def get_all_data_by_key(self, key):
        """根据key获取全部数据"""
        data_len = self.client.llen(key)
        result = self.client.lrange(key, 0, data_len)
        return result

    def get_pending_block(self):
        """获取待处理的块"""
        return self.get_data_by_key(self.pending_block_key)

    def get_basic(self):
        """获取basic信息"""
        basic = self.client.get(self.basic_key)
        if not basic:
            item = '{"current_height": 0, "newest_height": 0, "update_time": 0}'
            self.client.set(self.basic_key, item)
            basic = self.client.get(self.basic_key)
        return json.loads(basic)

    def update_basic(self, item):
        """更新basic信息"""
        item["update_time"] = get_now()
        self.client.set(self.basic_key, json.dumps(item))

    def get_mempool_info(self):
        """获取mempool中的tx_ids"""
        redis_mempool = self.client.get(self.mempool_key)
        if not redis_mempool:
            redis_mempool_list = []
            self.client.set(self.mempool_key, json.dumps(list()))
        else:
            redis_mempool_list = json.loads(redis_mempool)
        return redis_mempool_list

    def update_mempool_info(self, mempool_tx_ids):
        """更新mempoll信息"""
        self.client.set(self.mempool_key, json.dumps(mempool_tx_ids))

    def save_cache_block(self, block_height, block_hash):
        """缓存已推送的块高度及hash，并设置有效期为12小时"""
        self.client.set(f"{self.coin}:{block_height}", block_hash)
        return self.client.expire(f"{self.coin}:{block_height}", 3600)

    def get_cache_block(self, block_height):
        """根据height获取块内容"""
        result = self.client.get(f"{self.coin}:{block_height}")
        return result.decode() if result else None

    def del_cache_block(self, block_height):
        """根据block_height获取块内容"""
        self.client.delete(f"{self.coin}:{block_height}")
