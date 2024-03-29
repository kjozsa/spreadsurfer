import hashlib
import hmac
import json
import math
import signal
from datetime import datetime
from random import randint

import asyncio
import websockets
from loguru import logger

from .timeutils import timestamp_now_ms

config = json.load(open('config.json'))

uri = config['binance']['wss_uri']
api_key = config['binance']['api_key']
secret_key = config['binance']['secret']

# uri = "wss://testnet.binance.vision/ws-api/v3"
# api_key = "eJANeyXf07voeeNVygX5uO8EEDryj6UkiMKSw6b5MOW8lhf6hEUWmVIeYFMvH67g"
# secret_key = "8BMZram2Le7iBcNZjyBo7yFHkpd8hYUUWUTyljZg1cTUBIkXDRruP39gFYPkHksL"

test_mode = config['orders']['test_mode']


class BinanceWebsocketConnector:
    def __init__(self):
        self.websocket = None

    async def start(self):
        logger.info('connecting to WSS server: {}', uri)
        self.websocket = await websockets.connect(uri)
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, loop.create_task, self.websocket.close())

    async def send_buy_order(self, order_nr, client_order_id, price, amount, limit, recv_window=None):
        limit_str = 'LIMIT' if limit else 'MARKET'
        try:
            order, timestamp_created_ms = await self.send_order(f'{client_order_id}', price, amount, buy=True, limit=limit, recv_window=recv_window)
            order_id = order['result']['orderId']
            logger.success('#{}. {} BUY ORDER PLACED!! order_id {} - at price {}, amount {}', order_nr, limit_str, client_order_id, price if limit else '?', amount)
            return order_id, client_order_id, timestamp_created_ms
        except Exception as e:
            logger.error('BUY order failed: {}', repr(e))
            return None, None, None

    async def send_sell_order(self, order_nr, client_order_id, price, amount, limit, recv_window=None):
        limit_str = 'LIMIT' if limit else 'MARKET'
        try:
            order, timestamp_created_ms = await self.send_order(f'{client_order_id}', price, amount, buy=False, limit=limit, recv_window=recv_window)
            order_id = order['result']['orderId']
            logger.success('#{}. {} SELL ORDER PLACED!! order_id {} - at price {}, amount {}', order_nr, limit_str, client_order_id, price if limit else '?', amount)
            return order_id, client_order_id, timestamp_created_ms
        except Exception as e:
            logger.error('SELL order failed: {}', repr(e))
            return None, None, None

    async def send_order(self, order_id, price, amount, buy, limit, recv_window):
        now = timestamp_now_ms()
        buy_sell = 'BUY' if buy else 'SELL'
        params = {
            'apiKey': api_key,
            'newClientOrderId': order_id,
            'newOrderRespType': 'ACK',
            'price': price,
            'quantity': amount,
            'recvWindow': recv_window,
            'side': buy_sell,
            'symbol': 'BTCUSDT',
            'timeInForce': 'GTC',
            'timestamp': now,
            'type': 'LIMIT' if limit else 'MARKET'
        }
        if not limit:  # market order has no price or timeInForce field
            params.pop('price')
            params.pop('timeInForce')

        if not recv_window:
            params.pop('recvWindow')

        request = self.sign(params, order_id, 'order.place')
        if not test_mode:
            await self.websocket.send(request)
            response_str = await self.websocket.recv()
            response = json.loads(response_str)
            if response['status'] != 200:
                if response['error']['code'] == -1099:
                    # order was processed after recv_window was over. Ignore this issue
                    raise Exception(f'recv_window limit {recv_window}ms exceeded (too slow order)')

                if response['error']['code'] == -2010:
                    raise Exception(response['error']['msg'])
                else:
                    logger.error('order request {} was : {}\nresponse was: {}', order_id, request, response)
                    raise Exception(f'order {order_id} failed to create: ' + response_str)
        else:
            logger.error('TEST order created: {}', request)
            random_id = randint(1000000, 9999999)
            response = {'id': f'test{random_id}', 'result': {'orderId': random_id}}
        return response, now

    async def cancel_all_orders(self, identifier):
        if test_mode:
            return

        params = {
            'apiKey': api_key,
            'symbol': 'BTCUSDT',
            'timestamp': math.floor(datetime.now().timestamp() * 1000)
        }
        request = self.sign(params, identifier, 'openOrders.cancelAll')
        try:
            await self.websocket.send(request)
            response = json.loads(await self.websocket.recv())
            if response['status'] != 200:
                logger.debug('failed to cancel all orders: {}', response)
        except Exception as e:
            logger.debug('exception while cancelling orders: {}', e)

    async def cancel_order(self, order_id):
        if test_mode:
            return

        params = {
            'apiKey': api_key,
            'orderId': order_id,
            'symbol': 'BTCUSDT',
            'timestamp': math.floor(datetime.now().timestamp() * 1000)
        }
        request = self.sign(params, order_id, 'openOrders.cancel')
        await self.websocket.send(request)
        response_str = await self.websocket.recv()
        if json.loads(response_str)['status'] != 200:
            logger.debug('failed to cancel order {}: {}, request was: {}', order_id, response_str, request)
            raise Exception(f'failed to cancel order {order_id}: ' + response_str)

    @staticmethod
    def sign(params, identifier, method):
        params_list = [f'{key}={params[key]}' for key in params.keys()]
        params_str = '&'.join(params_list)
        params['signature'] = hmac.new(bytes(secret_key, 'utf-8'), msg=bytes(params_str, 'utf-8'), digestmod=hashlib.sha256).hexdigest()
        return json.dumps({
            'id': identifier,
            'method': method,
            'params': params
        })
