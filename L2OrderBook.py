#
# L2OrderBook.py
#
#
# Live order book updated from the Coinbase Websocket Level 2 Channel

from public_client import PublicClient
from websocket_client import WebsocketClient
from sortedcontainers import SortedDict
from decimal import Decimal

import tkinter
class Colors:
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    RED = '\033[31m'
    END = '\033[0m'

class Level2OrderbookClient(WebsocketClient):    
   
    def __init__(self, products=['BTC-USD']):
        super().__init__(url='wss://ws-feed.pro.coinbase.com/', products=products, channels=['level2'])

    def on_open(self):
        # Set connection parameters.
        pass
    
    def on_message(self, msg):
        if msg['type'] == 'snapshot':
            self.bids = SortedDict({Decimal(k): Decimal(v) for (k, v) in msg['bids']})
            self.asks = SortedDict({Decimal(k): Decimal(v) for (k, v) in msg['asks']})
        
        if msg['type'] == 'l2update':
            
            for change in msg['changes']:
                price = Decimal(change[1])
                size = Decimal(change[2])

                # Buy messages affect bid side; sell messages affect ask side.
                orderbook_side = self.bids if change[0] == 'buy' else self.asks
                
                # If the qty becomes 0, we need to get rid of this item in the order book.
                if size == 0:
                    orderbook_side.pop(price)
                else:
                    # Overwrite entry. The size is the new amount of orders at this price; it is not a delta.
                    orderbook_side[price] = size
                
                best_bid = self.bids.keys()[-1]
                best_bid_txt = Colors.GREEN + '{0:.2f}'.format(self.bids.keys()[-1]) + Colors.END

                best_ask = self.asks.keys()[0]
                best_ask_txt = Colors.RED+ '{0:.2f}'.format(self.asks.keys()[0]) + Colors.END

                spread = '{0:.2f}'.format(best_ask - best_bid)
                spread = Colors.BLUE + spread + Colors.END


                print('{}\t {}\t{}'.format(best_ask_txt,spread,best_bid_txt))


if __name__ == '__main__':
    client = Level2OrderbookClient()
    client.start()