import os
from brokers.flattrade import FlattradeBroker

if __name__ == "__main__":
    broker = FlattradeBroker()
    if broker.authenticated:
        quote = broker.get_quote("NSE:NIFTY 50")
        print(quote)
