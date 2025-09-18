import os
from .base import BrokerBase
from .api_helper import NorenApiPy, Order
import sys

class FlattradeBroker(BrokerBase):
    def __init__(self):
        super().__init__()
        self.api = NorenApiPy()
        self.authenticate()

    def authenticate(self):
        usersession = os.environ.get('FLATTRADE_SESSION')
        userid = os.environ.get('FLATTRADE_USERID')
        ret = self.api.set_session(userid=userid, password='', usertoken=usersession)
        if ret:
            self.authenticated = True
        else:
            print(f"Flattrade authentication failed: {ret}")
            sys.exit(1)

    def place_order(self, symbol, quantity, price, transaction_type, order_type, variety, exchange, product, tag):
        buy_or_sell = 'B' if transaction_type == 'BUY' else 'S'

        order = Order(
            buy_or_sell=buy_or_sell,
            product_type=product,
            exchange=exchange,
            tradingsymbol=symbol,
            quantity=quantity,
            discloseqty=0,
            price_type=order_type,
            price=price if price else 0.0,
            trigger_price=None,
            retention='DAY',
            remarks=tag
        )

        ret = self.api.place_order(
            buy_or_sell=order.buy_or_sell,
            product_type=order.product_type,
            exchange=order.exchange,
            tradingsymbol=order.tradingsymbol,
            quantity=order.quantity,
            discloseqty=order.discloseqty,
            price_type=order.price_type,
            price=order.price,
            trigger_price=order.trigger_price,
            retention=order.retention,
            remarks=order.remarks,
            amo='NO'
        )
        if ret and ret.get('stat') == 'Ok':
            return ret.get('norenordno')
        else:
            print(f"Failed to place order: {ret}")
            return -1

    def get_quote(self, symbol):
        exchange, token = self.get_exchange_and_token(symbol)
        if not exchange or not token:
            return None

        ret = self.api.get_quotes(exchange=exchange, token=token)
        if ret and ret.get('stat') == 'Ok':
            return {
                symbol: {
                    'last_price': float(ret.get('lp')),
                    'instrument_token': ret.get('token')
                }
            }
        else:
            return None

    def get_exchange_and_token(self, symbol):
        if symbol == "NSE:NIFTY 50":
            return "NSE", "26000"

        parts = symbol.split(':')
        if len(parts) != 2:
            return None, None

        exchange, search_text = parts

        ret = self.api.searchscrip(exchange=exchange, searchtext=search_text)

        if ret and ret.get('stat') == 'Ok' and ret.get('values'):
            instrument = ret['values'][0]
            return instrument.get('exch'), instrument.get('token')

        return None, None

    def download_instruments(self):
        print("Downloading instruments for Flattrade (placeholder)...")
        pass
