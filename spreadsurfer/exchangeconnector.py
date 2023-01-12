import json

import ccxt.pro as ccxt

from loguru import logger


def connect_exchange():
    config = json.load(open('config.json'))
    logger.debug(f'using Binance apikey {config["binance"]["apiKey"]}')

    exchange = ccxt.binance({
        'apiKey': config['binance']['apiKey'],
        'secret': config['binance']['secret'],
        'newUpdates': True,
        'enableRateLimit': True
    })
    return exchange

