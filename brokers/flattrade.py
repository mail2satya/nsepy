import os
from .base import BrokerBase
from .api_helper import NorenApiPy, Order
import sys
import pandas as pd

class FlattradeBroker(BrokerBase):
    def __init__(self):
        super().__init__()
        print("Initializing FlattradeBroker...")
        self.api = NorenApiPy()
        self.authenticate()

    def authenticate(self):
        print("Authenticating FlattradeBroker...")
        usersession = os.environ.get('FLATTRADE_SESSION')
        userid = os.environ.get('FLATTRADE_USERID')
        ret = self.api.set_session(userid=userid, password='', usertoken=usersession)
        print(f"set_session returned: {ret}")
        if ret:
            self.authenticated = True
            print("Flattrade authentication successful.")
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

        print(f"Placing order: {order.__dict__}")
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
        print(f"get_quote called with symbol: {symbol}")
        exchange, token = self.get_exchange_and_token(symbol)
        if not exchange or not token:
            print(f"Could not find exchange and token for symbol: {symbol}")
            return None

        print(f"Getting quote for {symbol} with exchange {exchange} and token {token}")
        ret = self.api.get_quotes(exchange=exchange, token=token)
        print(f"get_quotes returned: {ret}")
        if ret and ret.get('stat') == 'Ok':
            return {
                symbol: {
                    'last_price': float(ret.get('lp')),
                    'instrument_token': ret.get('token')
                }
            }
        else:
            print(f"Failed to get quote: {ret}")
            return None

    def get_exchange_and_token(self, symbol):
        print(f"get_exchange_and_token called with symbol: {symbol}")
        if symbol == "NSE:NIFTY 50":
            return "NSE", "26000"

        parts = symbol.split(':')
        if len(parts) != 2:
            return None, None

        exchange, search_text = parts

        print(f"Searching for {search_text} in {exchange}")
        ret = self.api.searchscrip(exchange=exchange, searchtext=search_text)
        print(f"searchscrip returned: {ret}")

        if ret and ret.get('stat') == 'Ok' and ret.get('values'):
            instrument = ret['values'][0]
            return instrument.get('exch'), instrument.get('token')

        return None, None

    def download_instruments(self):
        print("Downloading instruments for Flattrade...")
        # There is no direct API call to download all instruments.
        # We will try to get all NFO instruments by searching for a generic term.
        # A more robust solution would be to find a downloadable instrument file if one exists.
        nfo_instruments = self.api.searchscrip(exchange='NFO', searchtext='')

        if nfo_instruments and nfo_instruments.get('stat') == 'Ok' and nfo_instruments.get('values'):
            self.instruments_df = pd.DataFrame(nfo_instruments['values'])
            print(f"Successfully downloaded {len(self.instruments_df)} NFO instruments.")
            # The strategy expects a 'tradingsymbol' column, but the API returns 'tsym'. Let's rename it.
            if 'tsym' in self.instruments_df.columns:
                self.instruments_df.rename(columns={'tsym': 'tradingsymbol'}, inplace=True)
        else:
            print("Failed to download NFO instruments. The strategy may not work correctly.")
            self.instruments_df = pd.DataFrame()

    def connect_websocket(self):
        print("Connecting to Flattrade websocket...")
        # The callbacks are set on the instance by the main script
        self.api.start_websocket(
            order_update_callback=self.on_order_update,
            subscribe_callback=self.on_ticks,
            socket_open_callback=self.on_connect
        )
