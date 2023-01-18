import hashlib
import hmac
import json
import math
import signal
from datetime import datetime

import asyncio
import websockets
from loguru import logger

config = json.load(open('config.json'))

# uri = "wss://testnet.binance.vision/ws-api/v3"
# api_key = "eJANeyXf07voeeNVygX5uO8EEDryj6UkiMKSw6b5MOW8lhf6hEUWmVIeYFMvH67g"
# secret_key = "8BMZram2Le7iBcNZjyBo7yFHkpd8hYUUWUTyljZg1cTUBIkXDRruP39gFYPkHksL"

uri = 'wss://ws-api.binance.com/ws-api/v3'
api_key = config['binance']['apiKey']
secret_key = config['binance']['secret']
test_mode = config['orders']['test_mode']


class BinanceWebsocketConnector:
    def __init__(self):
        self.websocket = None

    async def start(self):
        logger.info('connecting to WSS server: {}', uri)
        self.websocket = await websockets.connect(uri)
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, loop.create_task, self.websocket.close())

    async def send_buy_order(self, order_nr, wave_id, price, amount, new_orders, limit):
        limit_str = 'LIMIT' if limit else 'MARKET'
        try:
            order = await self.send_order('B-' + wave_id, price, amount, buy=True, limit=limit)
            logger.success('#{}. {} BUY ORDER PLACED!! wave {} - at price {}', order_nr, limit_str, wave_id, price if limit else '?')
            new_orders.append(order)
        except Exception as e:
            logger.error(e)

    async def send_sell_order(self, order_nr, wave_id, price, amount, new_orders, limit):
        limit_str = 'LIMIT' if limit else 'MARKET'
        try:
            order = await self.send_order('S-' + wave_id, price, amount, buy=False, limit=limit)
            logger.success('#{}. {} SELL ORDER PLACED!! wave {} - at price {}', order_nr, limit_str, wave_id, price if limit else '?')
            new_orders.append(order)
        except Exception as e:
            logger.error(e)

    async def send_order(self, order_id, price, amount, buy, limit):
        buy_sell = 'BUY' if buy else 'SELL'
        params = {
            'apiKey': api_key,
            'newOrderRespType': 'ACK',
            'price': price,
            'quantity': amount,
            # 'recvWindow': recv_window,
            'side': buy_sell,
            'symbol': 'BTCUSDT',
            'timeInForce': 'GTC',
            'timestamp': math.floor(datetime.now().timestamp() * 1000),
            'type': 'LIMIT' if limit else 'MARKET'
        }
        if not limit:  # market order has no price or timeInForce field
            params.pop('price')
            params.pop('timeInForce')

        request = self.sign(params, order_id, 'order.place')
        if not test_mode:
            await self.websocket.send(request)
            response = await self.websocket.recv()
            if json.loads(response)['status'] != 200:
                logger.error('order request was {} : {}', order_id, request)
                raise Exception('order ' + order_id + ' failed to create: ' + response)
        else:
            logger.error('TEST order created: {}', request)
            response = {'id': 'test12345'}
        return response

    async def cancel_orders(self, wave_id):
        params = {
            'apiKey': api_key,
            'symbol': 'BTCUSDT',
            'timestamp': math.floor(datetime.now().timestamp() * 1000)
        }
        request = self.sign(params, wave_id, 'openOrders.cancelAll')
        try:
            await self.websocket.send(request)
            response = await self.websocket.recv()
            if json.loads(response)['status'] != 200:
                logger.error('failed to cancel orders: {}', response)
        except Exception as e:
            logger.error('exception while cancelling orders: {}', e)

    @staticmethod
    def sign(params, order_id, method):
        params_list = [f'{key}={params[key]}' for key in params.keys()]
        params_str = '&'.join(params_list)
        params['signature'] = hmac.new(bytes(secret_key, 'utf-8'), msg=bytes(params_str, 'utf-8'), digestmod=hashlib.sha256).hexdigest()
        return json.dumps({
            'id': order_id,
            'method': method,
            'params': params
        })
