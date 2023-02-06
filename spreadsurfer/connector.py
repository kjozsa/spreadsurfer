import json

import ccxt.pro as ccxt

from loguru import logger


def connect_exchange():
    config = json.load(open('config.json'))
    logger.debug(f'using Binance apikey {config["binance"]["api_key"]}')

    exchange = ccxt.binance({
        'apiKey': config['binance']['api_key'],
        'secret': config['binance']['secret'],
        'newUpdates': True,
        'enableRateLimit': True
    })
    # exchange.set_sandbox_mode(True)
    return exchange

