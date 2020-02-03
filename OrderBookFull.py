#
# OrderBookFull.py
#
#
# Live order book updated from the Coinbase Websocket Full Channel


from public_client import PublicClient
from websocket_client import WebsocketClient
from sortedcontainers import SortedDict
import queue
from decimal import Decimal

class OrderBookFull(WebsocketClient):
    def __init__(self, product_id='BTC-USD'):
        super(OrderBookFull,self).__init__(products=product_id,channels=['full'])
        self.asks = SortedDict()
        self.bids = SortedDict()
        self._client = PublicClient()

        self.sequence = -2
        self.websocketQueue = queue.Queue()
    
    def get_product_id(self):
        return self.products[0]
    def on_open(self):
        self._sequence = -2
        print("-- Subscribed to OrderBook! --\n")

    def on_close(self):
        print("\n-- OrderBook Socket Closed! --")
    

    def on_message(self,message):
        self.processMessage(message)

    def on_sequence_gap(self, gap_start, gap_end):
        self.loadFullOrderBook()
        print("Error: messages missing ({} - {}). Re-initializing book at sequence {}.".format(gap_start, gap_end, self.sequence))
    
    
    
    def loadFullOrderBook(self):
        self.websocketQueue = queue.Queue()
        response = self._client.get_product_order_book(product_id=self.get_product_id(), level=3)
        self.asks = SortedDict()
        self.bids = SortedDict()
        #load reponse into asks and bids dicts
        for bid in response['bids']:
            self.addToOrderBook({
                'id':bid[2],
                'side':'buy',
                'price':Decimal(bid[0]),
                'size':Decimal(bid[1])
            })
        for ask in response['asks']:
            self.addToOrderBook({
                'id': ask[2],
                'side': 'sell',
                'price': Decimal(ask[0]),
                'size': Decimal(ask[1])
            })
        self.sequence = response['sequence']

        # Playback queued messages, discarding sequence numbers before or equal to the snapshot sequence number.
        for msg in self.getMessageFromQueue(self.websocketQueue):
            self.processMessage(msg)
        
        
    def getMessageFromQueue(self,q):
        while True:
            try:
                yield q.get_nowait()
            except queue.Empty:  # on python 2 use Queue.Empty
                break

    def processMessage(self,message):
        socketSequence = message.get('sequence',-1)
        
        if(self.sequence<0):
            self.websocketQueue.put(message)

        if self.sequence == -2:
            self.loadFullOrderBook()
            return
        if self.sequence == -1:
            #load order book is in process
            return
        if socketSequence <= self.sequence:
            #discard sequence numbers before or equal to the snapshot (rest api request) sequence number
            return
        elif socketSequence > self.sequence+1:
            #dropped a message, resync order book
            self.on_sequence_gap(self.sequence,socketSequence)
        
        msg_type = message['type']

        if msg_type == 'open':
            self.addToOrderBook(message)
        elif msg_type == 'done' and 'price' in message:
            self.removeFromOrderBook(message)
        elif msg_type == 'match':
            self.handleMatch(message)
        elif msg_type == 'change':
            self.change(message)
            print("change")
        self.sequence = socketSequence
    
    def addToOrderBook(self, order):
        order = {
            'id': order.get('order_id') or order['id'],
            'side': order['side'],
            'price': Decimal(order['price']),
            'size': Decimal(order.get('size') or order['remaining_size'])
        }
        if order['side'] == 'buy':
            bidsAtThisPrice = self.getBidsAtThisPrice(order['price'])
            if bidsAtThisPrice is None:
                bidsAtThisPrice = [order]
            else:
                bidsAtThisPrice.append(order)
            self.setBidsAtThisPrice(order['price'],bidsAtThisPrice)
        else:
            asksAtThisPrice = self.getAsksAtThisPrice(order['price'])
            if asksAtThisPrice is None:
                asksAtThisPrice = [order]
            else:
                asksAtThisPrice.append(order)
            self.setAsksAtThisPrice(order['price'], asksAtThisPrice)
    
    def removeFromOrderBook(self,order):
        price = Decimal(order['price'])
        if(order['side']=='buy'):
            bids = self.getBidsAtThisPrice(price)
            if bids is not None:
                #get a the list of bids at this prices that do not match the order ID we want to delete
                bids = [o for o in bids if o['id'] != order ['order_id']]
                if len(bids) > 0:
                    #set bids at this price to the list of bids at this price that are not equal to the order ID we want to remove
                    self.setBidsAtThisPrice(price, bids)
                else:
                    #There are no more bids at this price so remove it from the dictionary holding all the bids
                    self.removeBidsAtThisPrice(price)
        else:
            asks = self.getAsksAtThisPrice(price)
            if asks is not None:
                asks = [o for o in asks if o['id'] != order['order_id']]
                if len(asks) > 0:
                    self.setAsksAtThisPrice(price, asks)
                else:
                    self.removeAsksAtThisPrice(price)
    
    def handleMatch(self,order):
        price = Decimal(order['price'])
        size = Decimal(order['size'])
        if(order['side'] == 'buy'):
            bids = self.getBidsAtThisPrice(price)
            if not bids:
                return
            #assert bids[0]['id'] == order['maker_order_id']
            for bid in bids:
                if bid['id'] == order['maker_order_id']:
                    if(bid['size'] == size):
                        #remove this bid from list of bids at this price because match will result in size of zero
                        bidsAtThisPrice = [o for o in bids if o['id'] != bid['id']]
                        if len(bidsAtThisPrice) > 0:
                            #set bids at this price to the list of bids at this price that are not equal to the order ID we want to remove
                            self.setBidsAtThisPrice(price, bidsAtThisPrice)
                        else:
                            #There are no more bids at this price so remove it from the dictionary holding all the bids
                            self.removeBidsAtThisPrice(price)
                        break
                    else:
                        #decrement the bid size by size and set bids at this price
                        bid['size'] -= size
                        self.setBidsAtThisPrice(price, bids)
                        break
        else:
            asks = self.getAsksAtThisPrice(price)
            if not asks:
                return
            for ask in asks:
                if ask['id'] == order['maker_order_id']:
                    if(ask['size'] == size):
                        # remove this ask from list of asks at this price because match will result in size of zero
                        asksAtThisPrice = [o for o in asks if o['id'] != ask['id']]
                        if len(asksAtThisPrice)>0:
                            self.setAsksAtThisPrice(price,asksAtThisPrice)
                        else:
                            self.removeAsksAtThisPrice(price)
                        break
                    else:
                        ask['size'] -= size
                        self.setAsksAtThisPrice(price, asks)
                        break
    
    def change(self,order):
        try:
            new_size = Decimal(order['new_size'])
        except KeyError:
            return

        try:
            #price of null indicates market order
            price = Decimal(order['price'])
        except KeyError:
            return
        if order['side'] == 'buy':
            bids = self.getBidsAtThisPrice(price)
            if bids is None or not any(o['id'] == order['order_id'] for o in bids):
                return
            index = [b['id'] for b in bids].index(order['order_id'])
            bids[index]['size'] = new_size
            self.setBidsAtThisPrice(price, bids)
        else:
            asks = self.getAsksAtThisPrice(price)
            if asks is None or not any(o['id'] == order['order_id'] for o in asks):
                return
            index = [a['id'] for a in asks].index(order['order_id'])
            asks[index]['size'] = new_size
            self.setAsksAtThisPrice(price, asks)
       
        # implementation to reduce redundancy of if side == buy/sell 
        # tree = self._asks if order['side'] == 'sell' else self._bids
        # node = tree.get(price)

        # if node is None or not any(o['id'] == order['order_id'] for o in node):
        #     return

        



            

                        


    def getBidsAtThisPrice(self,price):
        return self.bids.get(price)
    
    def setBidsAtThisPrice(self,price,bids):
        self.bids[price] = bids
    
    def removeBidsAtThisPrice(self,price):
        del self.bids[price]

    
    def getAsksAtThisPrice(self,price):
        return self.asks.get(price)
    
    def setAsksAtThisPrice(self,price,bids):
        self.asks[price] = bids
    
    def removeAsksAtThisPrice(self,price):
        del self.asks[price]
    
    def getTopBid(self):
        return self.bids.peekitem(-1)[0]
    def getTopAsk(self):
        return self.asks.peekitem(0)[0]

        

if __name__ == '__main__':
    import sys
    import time
    import datetime as dt

    class Colors:
        BLUE = '\033[94m'
        GREEN = '\033[92m'
        RED = '\033[31m'
        END = '\033[0m'

    
    class OrderBookConsole(OrderBookFull):
        ''' Logs real-time changes to the bid-ask spread to the console '''

        def __init__(self, product_id=None):
            super(OrderBookConsole, self).__init__(product_id=product_id)

            # latest values of bid-ask spread
            self.topBid = None
            self.topAsk = None
        
        def on_message(self, message):
            super(OrderBookConsole, self).on_message(message)

            self.topBid = self.getTopBid()
            self.topAsk = self.getTopAsk()
            best_ask_txt = Colors.RED+ '{0:.2f}'.format(self.topAsk) + Colors.END
            best_bid_txt = Colors.GREEN+ '{0:.2f}'.format(self.topBid) + Colors.END
            print('{} \t {}'.format(best_ask_txt,best_bid_txt))


    order_book = OrderBookConsole()
    order_book.start()
    
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        order_book.close()
    

    if order_book.error:
        sys.exit(1)
    else:
        sys.exit(0)



        

