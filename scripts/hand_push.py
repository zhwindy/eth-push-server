from lib.event import Event
from argparse import ArgumentParser
from core.globals import G_CFG, G_LOGGER

if __name__ == '__main__':
    """
    # 开发测试
    python -m scripts.hand_push tomo http://161.117.89.129:8545 -n 1740124 -c 14
    python -m scripts.hand_push ltc http://coldlar:coldwallet@39.106.44.136:25802/ -m dev -n 1740124 -c 14
    
    # 线上执行
    python -m scripts.hand_push ltc http://coldlar:coldwallet@172.17.67.156:25802/ -m prod -n 1740124 -c 14
    """
    # 从命令行参数读取货币和rpc地址
    parser = ArgumentParser()
    parser.description = "数字货币交易数据推送服务"
    parser.add_argument("coin", help="指定要开启推送的货币名称")
    parser.add_argument("rpc", help="节点rpc请求地址")
    parser.add_argument("-k", "--routing_key", help="设置routing_key")
    parser.add_argument("-e", "--exchange_name", help="设置exchange_name")
    parser.add_argument("-v", "--vhost", help="设置vhost")
    parser.add_argument("-mh", "--mq_host", help="设置mq地址")
    parser.add_argument("-mu", "--mq_user", help="设置mq账号")
    parser.add_argument("-mp", "--mq_password", help="设置mq密码")
    parser.add_argument("-m", "--mode", help="设置运行模式，dev开发环境,prod生产环境")
    parser.add_argument("-p", "--process", type=int, help="设置进程数量,默认1")
    parser.add_argument("-n", "--block_num", type=int, help="起始块号")
    parser.add_argument("-c", "--count", type=int, help="推送块数")
    args = parser.parse_args()
    print(args)

    # 根据传入的参数更新全局配置
    G_CFG.coin.coin_dict["name"] = args.coin
    process = args.process if args.process else 1
    G_CFG.coin.coin_dict["process"] = process
    G_CFG.rpc.rpc_dict["url"] = args.rpc
    G_CFG.mq.mq_dict["routing_key"] = args.routing_key if args.routing_key else args.coin
    if args.exchange_name:
        G_CFG.mq.mq_dict["exchange_name"] = args.exchange_name
    if args.vhost:
        G_CFG.mq.mq_dict["vhost"] = args.vhost
    if args.mq_host:
        G_CFG.mq.mq_dict["host"] = args.mq_host
    if args.mq_user:
        G_CFG.mq.mq_dict["username"] = args.mq_user
    if args.mq_password:
        G_CFG.mq.mq_dict["password"] = args.mq_password
    if args.mode:
        G_CFG.coin.coin_dict["mode"] = args.mode
    G_CFG.mysql.mysql_dict["db"] = args.coin
    G_CFG.log.log_dict["filename"] = f"log/{args.coin}.log"
    G_CFG.message.message_dict["monitor_path"] = f"log/{args.coin}.txt"
    mode = G_CFG.coin.coin_dict["mode"]
    if mode not in ["prod", "dev"]:
        G_LOGGER.info("未知的运行模式")
        exit()

    # 手动推送区块数据
    coin_push = Event.coin_push()
    coin_name = args.coin
    block_num = args.block_num
    count = args.count
    print('手动推送区块数据，coin: {}, start_block: {}, end_blcok: {}'.format(coin_name, block_num, block_num + count - 1))
    coin_push.push_sync(block_num, count)
