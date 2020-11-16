import atexit
import traceback
import pymysql
from DBUtils import PooledDB
from lib.tool import get_now
from collections import namedtuple
from lib.tool import Single, CachedProperty
from core.globals import G_LOGGER, G_PUSH_TEMPLATE, G_ETH_MULTI_PUSH_TEMPLATE


class MySQLBase:

    def __init__(self, cfg):
        self.cfg = cfg

    @CachedProperty
    def pool(self):
        """
        返回数据库连接池对象
        """
        def create_pool(obj):
            """
            创建数据库连接池
            """
            G_LOGGER.debug("创建MySQL数据库连接池")
            pool = PooledDB.PooledDB(pymysql, **obj.cfg.mysql.mysql_dict)
            atexit.register(obj.close)
            return pool

        def create_db(obj):
            """
            创建数据库
            """
            G_LOGGER.debug("没有数据库，创建新的数据库")
            new_db_config = obj.cfg.mysql.mysql_dict.copy()
            db_name = new_db_config.pop("db")
            connect = pymysql.Connect(**new_db_config)
            cursor = connect.cursor()
            sql = f"""
                CREATE DATABASE IF NOT EXISTS {db_name} DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            """
            cursor.execute(sql)
            connect.commit()
            connect.close()
            cursor.close()

        try:
            return create_pool(self)
        except pymysql.err.InternalError as e:
            if str(e).find("Unknown database") > -1:
                create_db(self)
            return create_pool(self)

    def close(self):
        G_LOGGER.debug('准备关闭MySQL数据库.')
        if self.pool:
            self.pool.close()
            G_LOGGER.debug('已关闭MySQL数据库.')
            return
        G_LOGGER.debug('MySQL数据库未打开, 不需要关闭.')

    def __enter__(self):
        con = self.pool.connection()
        cursor = con.cursor()
        return cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if any([exc_type, exc_val, exc_tb]):
            msg = "向数据库请求错误, 错误原因: {}\n错误详情: {}".format(exc_val, exc_tb)
            G_LOGGER.error(msg)
            raise Exception(msg)


class SqlBase:
    TABLE_NAME = ""

    def __init__(self, father):
        self.father = father

    def create_table(self, sql):
        tables = self.father.fetch_all("SHOW TABLES")
        tables = [table[0] for table in tables]
        if self.TABLE_NAME not in tables:
            self.father.query(sql)


class MySQL(MySQLBase, Single):
    auto_commit = True
    connect = None
    cursor = None

    class MySQLQuery(Single):
        def __init__(self, father):
            self.father = father

        def __enter__(self):
            self.con = self.father.pool.connection()
            cursor = self.con.cursor()
            return cursor

        def __exit__(self, exc_type, exc_val, exc_tb):
            error = any([exc_type, exc_val, exc_tb])
            if error:
                msg = "向数据库请求错误, 错误原因: {}\n错误详情: {}".format(exc_val, exc_tb)
                G_LOGGER.error(msg)
                self.con.rollback()
                raise Exception(msg)
            self.con.commit()
            self.con.close()

    def __init__(self, cfg):
        super().__init__(cfg)

    def query(self, sql, *args):
        G_LOGGER.debug("要执行的sql: '{}', args: {}".format(sql, args))
        with self.MySQLQuery(self) as cursor:
            cursor.execute(sql, *args)
            return cursor

    def fetch_all(self, sql, *args, obj=False):
        """针对于select, 拿出所有查询的数据"""
        cursor = self.query(sql, *args)
        if obj and cursor.rowcount:
            Fetch = namedtuple('Fetch', [row[0] for row in cursor.description])
            return [Fetch(*row) for row in cursor.fetchall()]
        return [row for row in cursor.fetchall()]

    def fetch_all_iter(self, sql, *args, obj=False):
        """针对于select, 拿出所有查询的数据"""
        cursor = self.query(sql, *args)
        if obj and cursor.rowcount:
            Fetch = namedtuple('Fetch', [row[0] for row in cursor.description])
            for row in cursor.fetchall():
                yield Fetch(*row)
            return None
        for row in cursor.fetchall():
            yield row

    def fetch_one(self, sql, *args, obj=False):
        """用于查询, 使用连接池中的数据, 只拿到一个数据"""
        cursor = self.query(sql, *args)
        if obj and cursor.rowcount:
            Fetch = namedtuple('Fetch', [row[0] for row in cursor.description])
            return Fetch(*cursor.fetchone())
        return cursor.fetchone()

    def safe_con(self):
        """用于事务, 必须在一个con下面"""
        if not self.connect or self.connect.close:
            self.connect = self.pool.connection()
        if not self.cursor or self.cursor.close:
            self.cursor = self.connect.cursor()
        return self.cursor

    def begin(self):
        self.connect.begin()

    def commit(self):
        self.connect.commit()

    def rollback(self):
        self.connect.rollback()

    def execute(self, sql, *args):
        """执行, 只返回影响的行数"""
        cursor = self.query(sql, *args)
        if cursor:
            return cursor.rowcount
        return 0

    def tr_query(self, sql, *args):
        G_LOGGER.debug("要执行的sql: '{}', args: {}".format(sql, args))
        cursor = self.safe_con()
        try:
            cursor.execute(sql, *args)
        except Exception as e:
            G_LOGGER.error('调用数据库错误, 原因: {}, 详情: {}'.format(e, traceback.format_exc()))
            return None
        return cursor

    def tr_execute(self, sql, *args):
        """执行, 只返回影响的行数"""
        cursor = self.tr_query(sql, *args)
        if cursor:
            return cursor.rowcount
        return 0


class BasicSql(SqlBase):
    TABLE_NAME = "basic"
    CREATE_SQL = f"""
        CREATE TABLE {TABLE_NAME} (
            `id` INT(32) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '记录id,自增长',
            `create_time` DATETIME default now() COMMENT '创建时间',
            `chain_name` CHAR(32) NOT NULL COMMENT '链名称',
            `current_height` int(32) NOT NULL DEFAULT 0,
            `newest_height` int(32) NOT NULL DEFAULT 0,
            `rollback_count` int(32) NOT NULL DEFAULT 0,
            `update_time` DATETIME default now() COMMENT '更新时间',
            `active` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否激活',
            PRIMARY KEY (`id`),
            UNIQUE KEY `chain_name` (`chain_name`) USING BTREE
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4
    """

    def __init__(self, father):
        super().__init__(father)
        self.create_table(self.CREATE_SQL)

    def get_basic(self, coin_name):
        """
        获取basic信息
        """
        sql = f"""
            SELECT
                id, current_height, newest_height, rollback_count
            FROM
                {self.TABLE_NAME}
            WHERE
                chain_name = '{coin_name}' and active=1
            LIMIT 1
        """
        return self.father.fetch_one(sql, obj=True)

    def update_basic(self, id, current_height, newest_height):
        """
        更新basic信息
        """
        sql = f"""
            UPDATE
                {self.TABLE_NAME}
            SET
                current_height={current_height}, newest_height={newest_height}, update_time=now()
            WHERE
                id={id}
        """
        return self.father.execute(sql)


class TransactionSql(SqlBase):
    TABLE_NAME = "transaction"
    CREATE_SQL = f"""
        CREATE TABLE {TABLE_NAME} (
            `id` INT(32) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '记录id,自增长',
            `create_time` DATETIME default now() COMMENT '创建时间',
            `chain_name` CHAR(32) NOT NULL COMMENT '链名称',
            `block_height` int(32) NOT NULL COMMENT '区块高度',
            `txid` CHAR(80) NOT NULL COMMENT '交易哈希',
            PRIMARY KEY (`id`),
            KEY `chain_name` (`chain_name`) USING BTREE,
            KEY `txid` (`txid`) USING BTREE
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4
    """

    def __init__(self, father):
        super().__init__(father)
        self.create_table(self.CREATE_SQL)

    def save_transactions(self, transactions):
        """
        保存tx
        """
        sql = "INSERT INTO {} (`chain_name`, `block_height`, `txid`) values {}".format(self.TABLE_NAME, ",".join(transactions))
        return self.father.execute(sql)


class LogSql(SqlBase):
    TABLE_NAME = "log"
    CREATE_SQL = (f"CREATE TABLE IF NOT EXISTS `{TABLE_NAME}`  (\n"
                  f"  `id` int(11) NOT NULL AUTO_INCREMENT,\n"
                  f"  `block_number` varchar(20) NOT NULL,\n"
                  f"  `tx_id` varchar(150) NOT NULL,\n"
                  f"  `type` varchar(10) NOT NULL,\n"
                  f"  `from_addr` varchar(100) NULL DEFAULT NULL,\n"
                  f"  `to_addr` varchar(100) NOT NULL,\n"
                  f"  `time` varchar(30) NOT NULL,\n"
                  f"  `amount` varchar(30) NOT NULL,\n"
                  f"  `push_time` varchar(20) NOT NULL,\n"
                  f"  `c_key` varchar(150) NOT NULL UNIQUE,\n"
                  f"  PRIMARY KEY (`id`) USING BTREE\n"
                  f") ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;")

    def __init__(self, father):
        super().__init__(father)
        self.create_table(self.CREATE_SQL)

    def insert_log(self, d):
        sql = (f"INSERT IGNORE INTO {self.TABLE_NAME}(`block_number`, `tx_id`, `type`, `from_addr`, `to_addr`, `time`, `amount`, "
               f"`push_time`, `c_key`) VALUES ('{d['BlockNumber']}', '{d['Txid']}', '{d['Type']}', '{d['From']}', "
               f"'{d['To']}', '{d['Time']}', '{d['Amount']}', '{get_now()}', '{d['c_key']}')")
        return self.father.execute(sql)

    def get_log(self):
        sql = (f"SELECT `block_number`, `tx_id`, `type`, `from_addr`, `to_addr`, `time`, `amount`, `c_key` " 
               f"FROM {self.TABLE_NAME} ORDER BY id LIMIT 10")
        result = self.father.fetch_all(sql, obj=True)
        # 删除已经取出的数据
        del_sql = (f"DELETE FROM {self.TABLE_NAME} WHERE id IN (SELECT x.id FROM (select id from {self.TABLE_NAME} "
                   f"order by id LIMIT 10) as x)")
        self.father.execute(del_sql)
        data_list = []
        for r in result:
            data = G_PUSH_TEMPLATE.copy()
            data["BlockNumber"] = r.block_number
            data["Txid"] = r.tx_id
            data["Type"] = r.type
            data["From"] = r.from_addr
            data["To"] = r.to_addr
            data["Time"] = r.time
            data["Amount"] = r.amount
            data["TxIndex"] = r.c_key.split("-")[1]
            data_list.append(data)
        return data_list


class MySQLDB(MySQL):

    def __init__(self, cfg):
        super().__init__(cfg)
        self.cfg = cfg

    @CachedProperty
    def basic(self):
        return BasicSql(self)

    @CachedProperty
    def log(self):
        return LogSql(self)

    @CachedProperty
    def transaction(self):
        return TransactionSql(self)

    @CachedProperty
    def contract(self):
        return ContractSql(self)

    @CachedProperty
    def eth_multi_contract(self):
        return EthMultiContractSql(self)

    @CachedProperty
    def trade(self):
        return TradeSql(self)

    @CachedProperty
    def eth_multi_log(self):
        return EthMultiLogSql(self)


class ContractSql(SqlBase):
    TABLE_NAME = "contract"
    CREATE_SQL = (f"CREATE TABLE IF NOT EXISTS `{TABLE_NAME}`  (\n"
                  f"  `id` int NOT NULL AUTO_INCREMENT,\n"
                  f"  `contract` varchar(100) NOT NULL UNIQUE,\n"
                  f"  `timestamp` varchar(20) NOT NULL,\n"
                  f"  `hash` varchar(100) NOT NULL,\n"
                  f"  `blockNumber` varchar(30) NOT NULL,\n"
                  f"  PRIMARY KEY (`id`) USING BTREE\n"
                  f") ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;")

    def __init__(self, father):
        super().__init__(father)
        self.create_table(self.CREATE_SQL)

    def insert_contract(self, d):
        sql = (f"INSERT IGNORE INTO {self.TABLE_NAME}(`contract`, `timestamp`, `hash`, `blockNumber`) "
               f"VALUES ('{d['contract']}', '{d['timestamp']}', '{d['hash']}', '{d['blockNumber']}')")
        return self.father.execute(sql)

    def get_by_contract(self, contract):
        sql = (f"SELECT `contract`, `timestamp`, `hash`, `blockNumber` "
               f"FROM {self.TABLE_NAME} WHERE `contract`='{contract}'")
        result = self.father.fetch_one(sql, obj=True)
        return result


class TradeSql(SqlBase):
    TABLE_NAME = "trade"
    # done 0最初状态，1通过状态，2拒绝状态
    # is_push 0未推送，1推送
    CREATE_SQL = (f"CREATE TABLE IF NOT EXISTS `{TABLE_NAME}`  (\n"
                  f"  `id` int NOT NULL AUTO_INCREMENT,\n"
                  f"  `operationId` varchar(100) NULL,\n"
                  f"  `type` varchar(20) NOT NULL,\n"
                  f"  `contract` varchar(100) NOT NULL,\n"
                  f"  `from_addr` varchar(100) NOT NULL,\n"
                  f"  `to_addr` varchar(100),\n"
                  f"  `value` varchar(100),\n"
                  f"  `confirm` int NOT NULL default 1,\n"
                  f"  `revoke` int NOT NULL default 0,\n"
                  f"  `done` tinyint NOT NULL default 0,\n"
                  f"  `data` text,\n"
                  f"  `timestamp` varchar(20) NOT NULL,\n"
                  f"  `hash` varchar(100) NOT NULL UNIQUE,\n"
                  f"  `blockNumber` varchar(30) NOT NULL,\n"
                  f"  `is_push` tinyint not null default 0,\n"
                  f"  `push_time` varchar(20),\n"
                  f"  PRIMARY KEY (`id`) USING BTREE,\n"
                  f"  INDEX (`operationId`),\n"
                  f"  INDEX (`contract`)\n"
                  f") ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;")

    def __init__(self, father):
        super().__init__(father)
        self.create_table(self.CREATE_SQL)

    def insert_trade(self, d):
        sql = (f"INSERT IGNORE INTO {self.TABLE_NAME}(`operationId`, `type`, `contract`, `from_addr`, `to_addr`, `value`, "
               f"`data`, `timestamp`, `hash`, `blockNumber`) VALUES ('{d['operationId']}', '{d['type']}', "
               f"'{d['contract']}', '{d['from_addr']}', '{d['to_addr']}', '{d['value']}', '{d['data']}', '{d['timestamp']}', "
               f"'{d['hash']}', '{d['blockNumber']}')")
        return self.father.execute(sql)

    def update_trade_block_number(self, d):
        sql = f"UPDATE {self.TABLE_NAME} SET `blockNumber`='{d['blockNumber']}' WHERE `hash`='{d['hash']}'"
        return self.father.execute(sql)

    def get_by_hash(self, tx_hash):
        sql = (f"SELECT `operationId`, `contract`, `from_addr`, `to_addr`, `value`, `confirm`, `revoke`, `done`, `data`,"
               f"`timestamp`, `hash`, `blockNumber`, `is_push`, `push_time` FROM {self.TABLE_NAME} WHERE `hash`='{tx_hash}'")
        return self.father.fetch_one(sql, obj=True)

    def get_by_operation_id(self, operation_id, contract):
        sql = (f"SELECT `operationId`, `contract`, `from_addr`, `to_addr`, `value`, `confirm`, `revoke`, `done`, `data`,"
               f"`timestamp`, `hash`, `blockNumber`, `is_push` FROM {self.TABLE_NAME} WHERE `operationId`='{operation_id}' "
               f"AND `type`='execute' AND `contract`='{contract}'")
        return self.father.fetch_one(sql, obj=True)

    def update_execute_by_operation_id(self, d, operation_id, contract):
        sql = (f"UPDATE {self.TABLE_NAME} SET `confirm`={d['confirm']}, `revoke`={d['revoke']}, `done`={d['done']} "
               f"WHERE `operationId`='{operation_id}' AND `type`='execute' AND `contract`='{contract}'")
        return self.father.execute(sql)

    def update_by_tx_hash(self, d, tx_hash):
        sql = (f"UPDATE {self.TABLE_NAME} SET `operationId`='{d['operationId']}', `blockNumber`='{d['blockNumber']}', "
               f"`timestamp`='{d['timestamp']}', `is_push`={d['is_push']}, `push_time`='{d['push_time']}' "
               f"WHERE `hash`='{tx_hash}'")
        return self.father.execute(sql)

    def update_done_by_tx_hash(self, done, tx_hash):
        sql = f"UPDATE {self.TABLE_NAME} SET `done`={done} WHERE `hash`='{tx_hash}'"
        return self.father.execute(sql)


class EthMultiLogSql(SqlBase):
    TABLE_NAME = "eth_multi_log"
    CREATE_SQL = (f"CREATE TABLE IF NOT EXISTS `{TABLE_NAME}`  (\n"
                  f"  `id` int(11) NOT NULL AUTO_INCREMENT,\n"
                  f"  `operationId` varchar(100) NULL,\n"
                  f"  `Txid` varchar(100) NOT NULL,\n"
                  f"  `owners` text NOT NULL,\n"
                  f"  `isPayer` tinyint NOT NULL,\n"
                  f"  `fromAddr` varchar(100) NOT NULL,\n"
                  f"  `toAddr` varchar(100) NOT NULL,\n"
                  f"  `multContract` varchar(100) NOT NULL,\n"
                  f"  `signN` int NOT NULL,\n"
                  f"  `maxN` tinyint NOT NULL,\n"
                  f"  `height` int NOT NULL,\n"
                  f"  `time` varchar(20) NOT NULL,\n"
                  f"  `value` varchar(100) NOT NULL,\n"
                  f"  `confirmed` varchar(10) NOT NULL,\n"
                  f"  `codeType` varchar(10) not null default 0,\n"
                  f"  `contract` varchar(100) NULL,\n"
                  f"  `c_key` varchar(150) NOT NULL UNIQUE,\n"
                  f"  PRIMARY KEY (`id`) USING BTREE\n"
                  f") ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;")

    def __init__(self, father):
        super().__init__(father)
        self.create_table(self.CREATE_SQL)

    def insert_log(self, d):
        sql = (f"INSERT IGNORE INTO {self.TABLE_NAME}(`operationId`, `Txid`, `owners`, `isPayer`, `fromAddr`, "
               f"`toAddr`, `multContract`, `signN`, `maxN`, `height`, `time`, `value`, `confirmed`, `codeType`, "
               f"`contract`, `c_key`) VALUES ('{d['operationId']}', '{d['Txid']}', '{'|'.join(d['owners'])}', '{d['isPayer']}', "
               f"'{d['from']}', '{d['to']}', '{d['multContract']}', '{d['signN']}', '{d['maxN']}', '{d['height']}', "
               f"'{d['time']}', '{d['value']}', '{d['confirmed']}', '{d['codeType']}', '{d['contract']}', "
               f"'{d['c_key']}')")
        return self.father.execute(sql)

    def get_log(self):
        sql = (f"SELECT `operationId`, `Txid`, `owners`, `isPayer`, `fromAddr`, `toAddr`, `multContract`, `signN`, "
               f"`maxN`, `height`, `time`, `value`, `confirmed`, `codeType`, `contract`, `c_key` "
               f"FROM {self.TABLE_NAME} ORDER BY id LIMIT 10")
        result = self.father.fetch_all(sql, obj=True)
        # 删除已经取出的数据
        del_sql = (f"DELETE FROM {self.TABLE_NAME} WHERE id IN (SELECT x.id FROM (select id from {self.TABLE_NAME} "
                   f"order by id LIMIT 10) as x)")
        self.father.execute(del_sql)
        data_list = []
        for r in result:
            data = G_ETH_MULTI_PUSH_TEMPLATE.copy()
            data["operationId"] = r.operationId
            data["Txid"] = r.Txid
            data["owners"] = r.owners
            data["isPayer"] = r.isPayer
            data["from"] = r.fromAddr
            data["to"] = r.toAddr
            data["multContract"] = r.multContract
            data["signN"] = r.signN
            data["maxN"] = r.maxN
            data["height"] = r.height
            data["time"] = r.time
            data["value"] = r.value
            data["confirmed"] = r.confirmed
            data["codeType"] = r.codeType
            data["contract"] = r.contract
            data["TxIndex"] = r.c_key.split("-")[1]
            data_list.append(data)
        return data_list


class EthMultiContractSql(SqlBase):
    TABLE_NAME = "eth_multi_contract"
    CREATE_SQL = (f"CREATE TABLE IF NOT EXISTS `{TABLE_NAME}`  (\n"
                  f"  `id` int NOT NULL AUTO_INCREMENT,\n"
                  f"  `contract` varchar(100) NOT NULL UNIQUE,\n"
                  f"  `needSign` varchar(20) NOT NULL,\n"
                  f"  `maxSign` varchar(20) NOT NULL,\n"
                  f"  `dayLimit` varchar(100) NOT NULL,\n"
                  f"  `owners` text NOT NULL,\n"
                  f"  `timestamp` varchar(20) NOT NULL,\n"
                  f"  `hash` varchar(100) NOT NULL,\n"
                  f"  `blockNumber` varchar(30) NOT NULL,\n"
                  f"  PRIMARY KEY (`id`) USING BTREE\n"
                  f") ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;")

    def __init__(self, father):
        super().__init__(father)
        self.create_table(self.CREATE_SQL)

    def insert_contract(self, d):
        sql = (f"INSERT IGNORE INTO {self.TABLE_NAME}(`contract`, `needSign`, `maxSign`, `dayLimit`, `owners`, "
               f"`timestamp`, `hash`, `blockNumber`) VALUES ('{d['contract']}', '{d['needSign']}', '{d['maxSign']}', "
               f"'{d['dayLimit']}', '{d['owners']}', '{d['timestamp']}', '{d['hash']}', '{d['blockNumber']}')")
        return self.father.execute(sql)

    def get_by_contract(self, contract):
        sql = (f"SELECT `contract`, `needSign`, `maxSign`, `dayLimit`, `owners`, `timestamp`, `hash`, `blockNumber` "
               f"FROM {self.TABLE_NAME} WHERE `contract`='{contract}'")
        result = self.father.fetch_one(sql, obj=True)
        return result
