#!/usr/bin/env python
from lib.event import Event
from multiprocessing import Pool
from argparse import ArgumentParser
from core.globals import G_CFG, G_LOGGER


if __name__ == "__main__":
    try:
        # 从命令行参数读取货币和rpc地址
        parser = ArgumentParser()
        parser.description = "数字货币交易数据推送服务"
        parser.add_argument("coin", help="指定要开启推送的货币名称")
        parser.add_argument("rpc", help="节点rpc请求地址")
        parser.add_argument("-m", "--mode", help="设置运行模式，dev开发环境,prod生产环境")
        parser.add_argument("-p", "--process", type=int, help="设置进程数量,默认1")
        args = parser.parse_args()
        # 根据传入的参数更新全局配置
        G_CFG.coin.coin_dict["name"] = args.coin
        process = args.process if args.process else 0
        G_CFG.coin.coin_dict["process"] = process
        G_CFG.rpc.rpc_dict["url"] = args.rpc
        if args.mode:
            G_CFG.coin.coin_dict["mode"] = args.mode
        G_CFG.log.log_dict["filename"] = f"log/{args.coin}.log"
        mode = G_CFG.coin.coin_dict["mode"]
        if mode not in ["prod", "dev"]:
            G_LOGGER.info("未知的运行模式")
            exit()
        if args.process and args.process > 1:
            events = [Event.push_process, Event.mempool_process, Event.fetch_process]
        else:
            events = [Event.push_process, Event.mempool_process]
        process_len = len(events) + process
        # 开启多进程
        pool = Pool(processes=process_len)
        for e in events:
            result = pool.apply_async(e)
            if args.process and args.process > 1 and e == Event.fetch_process:
                for i in range(args.process - 1):
                    result = pool.apply_async(e)
        pool.close()
        pool.join()
    except KeyboardInterrupt:
        G_LOGGER.info("所有任务已被强行终止")
