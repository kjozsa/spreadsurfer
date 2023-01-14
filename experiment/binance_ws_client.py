import hashlib
import hmac
import json
import math
import signal
from datetime import datetime

import asyncio
import websockets
from loguru import logger

# logging.basicConfig(level=logging.DEBUG)

# testnet
api_key = "eJANeyXf07voeeNVygX5uO8EEDryj6UkiMKSw6b5MOW8lhf6hEUWmVIeYFMvH67g"
secret_key = "8BMZram2Le7iBcNZjyBo7yFHkpd8hYUUWUTyljZg1cTUBIkXDRruP39gFYPkHksL"
uri = "wss://testnet.binance.vision/ws-api/v3"

# live
# uri = "wss://ws-api.binance.com/ws-api/v3"


def build_request():
    params = {
        "apiKey": api_key,
        "newOrderRespType": "ACK",
        "price": "20700.00",
        "quantity": "0.001521",
        "recvWindow": 500,
        "side": "SELL",
        "symbol": "BTCUSDT",
        "timeInForce": "GTC",
        "timestamp": math.floor(datetime.now().timestamp() * 1000),
        "type": "LIMIT",
    }

    params_list = [f'{key}={params[key]}' for key in params.keys()]
    params_str = "&".join(params_list)
    params['signature'] = hmac.new(bytes(secret_key, 'utf-8'), msg=bytes(params_str, 'utf-8'), digestmod=hashlib.sha256).hexdigest()
    return json.dumps({
        "id": "T5T2Gbcke82iWy3FGolRv0P1RojU",
        "method": "order.place",
        "params": params
    })


async def client():
    logger.info('connecting to WSS server: {}', uri)
    async with websockets.connect(uri) as websocket:
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, loop.create_task, websocket.close())

        req = build_request()

        logger.info('sending WS message: {}', req)
        await websocket.send(req)

        msg = await websocket.recv()
        logger.info('recv WS message: {}', msg)

        logger.success('kill me now to test websocket close')
        while True:
            await asyncio.sleep(0)
    # logger.info('creating test order:')
    # await exchange.create_order('BTC/USDT', 'limit', 'buy', 0.001, 10000, {'test': True})


async def main():
    try:
        await client()
    except Exception as e:
        logger.exception(e)
        # await exchange.close()


if __name__ == '__main__':
    asyncio.run(main())
