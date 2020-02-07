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
        print("Error: messages missing ({} - {}). Re-initializing book".format(gap_start, gap_end))
    
    
    
    def loadFullOrderBook(self):
        # Reset queue. This queue will hold messages received from websocket while processing rest api request for full order book
        self.websocketQueue = queue.Queue()
        
        # Rest api call for full order book
        response = self._client.get_product_order_book(product_id=self.get_product_id(), level=3)
        
        # Reset our sorted dicts holding bid and ask orders
        self.asks = SortedDict()
        self.bids = SortedDict()

        # Load rest API reponse into asks and bids dicts
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
        
        # Update the current sequence 
        self.sequence = response['sequence']

        # Playback queued messages, discarding sequence numbers before or equal to the snapshot sequence number.
        for msg in self.getMessageFromQueue(self.websocketQueue):
            self.processMessage(msg)
        
    # This method retrieves each message from our queue until it is empty 
    def getMessageFromQueue(self,q):
        while True:
            try:
                yield q.get_nowait()
            except queue.Empty:  
                break

    # This method proccesses messages from websocket 
    def processMessage(self,message):
        socketSequence = message.get('sequence',-1)
        
        # A sequence less than zero indicates that we are processing a rest API request for the full orderbook
        if(self.sequence<0):
            # While we are proccessing rest request, add messages to queue to process later
            self.websocketQueue.put(message)

        if self.sequence == -2:
            # A sequence of -2 indicates that we need to perform our initial load of the full orderbook
            self.loadFullOrderBook()
            return
        if self.sequence == -1:
            # A sequence of -1 indicates that we are in the middle of processinng the rest API request for the full order book
            return
        if socketSequence <= self.sequence:
            # Discard sequence numbers before or equal to the sequence number returned by the rest API request 
            return
        elif socketSequence > self.sequence+1:
            # Dropped a message, resync order book
            self.on_sequence_gap(self.sequence,socketSequence)
        
        # Get message type
        msg_type = message['type']

        # Handle each message type
        if msg_type == 'open':
            self.addToOrderBook(message)
        elif msg_type == 'done' and 'price' in message:
            self.removeFromOrderBook(message)
        elif msg_type == 'match':
            self.handleMatch(message)
        elif msg_type == 'change':
            self.change(message)
        
        # Update sequence
        self.sequence = socketSequence
    
    # This method adds an order to our order book
    def addToOrderBook(self, order):
        order = {
            'id': order.get('order_id') or order['id'],
            'side': order['side'],
            'price': Decimal(order['price']),
            'size': Decimal(order.get('size') or order['remaining_size'])
        }
        if order['side'] == 'buy':
            # Get list of bid orders at this price
            bidsAtThisPrice = self.getBidsAtThisPrice(order['price'])
            if bidsAtThisPrice is None:
                # If there are no bid orders at this price, start a new list of orders at this price
                bidsAtThisPrice = [order]
            else:
                # If there are bid orders at this price add this order to the list 
                bidsAtThisPrice.append(order)
            # Update our sorted dictionary holding all bid orders 
            self.setBidsAtThisPrice(order['price'],bidsAtThisPrice)
        else:
            # Get list of ask orders at this price
            asksAtThisPrice = self.getAsksAtThisPrice(order['price'])
            if asksAtThisPrice is None:
                # If there are no ask orders at this price, start a new list of orders at this price
                asksAtThisPrice = [order]
            else:
                # If there are ask orders at this price, add this order to the list 
                asksAtThisPrice.append(order)
            # Update our sorted dictionary holding all ask orders 
            self.setAsksAtThisPrice(order['price'], asksAtThisPrice)
    
    # This method removes an order from our order book
    def removeFromOrderBook(self,order):
        price = Decimal(order['price'])
        if(order['side']=='buy'):
            bids = self.getBidsAtThisPrice(price)
            if bids is not None:
                # Get the list of bids at this prices that do not match the order ID we want to delete
                bids = [o for o in bids if o['id'] != order ['order_id']]
                if len(bids) > 0:
                    # Set bids at this price to the list of bids at this price that are not equal to the order ID we want to remove
                    self.setBidsAtThisPrice(price, bids)
                else:
                    # There are no more bids at this price, so remove it from the dictionary holding all the bids
                    self.removeBidsAtThisPrice(price)
        else:
            # Ask order
            asks = self.getAsksAtThisPrice(price)
            if asks is not None:
                # Get a the list of asks at this prices that do not match the order ID we want to delete
                asks = [o for o in asks if o['id'] != order['order_id']]
                if len(asks) > 0:
                    # Set asks at this price to the list of asks at this price that are not equal to the order ID we want to remove
                    self.setAsksAtThisPrice(price, asks)
                else:
                    # Thare are no more asks at this price so remove it from the dictionary holding all the asks
                    self.removeAsksAtThisPrice(price)
    
    # This method updates the order book when a match occurs
    def handleMatch(self,order):
        price = Decimal(order['price'])
        size = Decimal(order['size'])
        if(order['side'] == 'buy'):
            bids = self.getBidsAtThisPrice(price)
            if not bids:
                return
            for bid in bids:
                if bid['id'] == order['maker_order_id']:
                    if(bid['size'] == size):
                        # Remove this bid from our list of bids at this price because match will result in size of zero
                        bidsAtThisPrice = [o for o in bids if o['id'] != bid['id']]
                        if len(bidsAtThisPrice) > 0:
                            # Set bids at this price to the list of bids at this price that are not equal to the order ID we want to remove
                            self.setBidsAtThisPrice(price, bidsAtThisPrice)
                        else:
                            # There are no more bids at this price, so remove it from the dictionary holding all the bids
                            self.removeBidsAtThisPrice(price)
                        break
                    else:
                        # Decrement the bid size by size and set bids at this price
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
                        # Remove this ask from list of asks at this price because match will result in size of zero
                        asksAtThisPrice = [o for o in asks if o['id'] != ask['id']]
                        if len(asksAtThisPrice)>0:
                            # Set asks at this price to the list of asks at this price that are not equal to the order ID we want to remove
                            self.setAsksAtThisPrice(price,asksAtThisPrice)
                        else:
                            # There are no more asks at this price so remove it from the dictionary holding all the asks
                            self.removeAsksAtThisPrice(price)
                        break
                    else:
                        # Decrement the asks size by size and set asks at this price
                        ask['size'] -= size
                        self.setAsksAtThisPrice(price, asks)
                        break
    
    def change(self,order):
        try:
            new_size = Decimal(order['new_size'])
        except KeyError:
            return
        try:
            # Price of null indicates market order
            price = Decimal(order['price'])
        except KeyError:
            return
        
        if order['side'] == 'buy':
            bids = self.getBidsAtThisPrice(price)
            if bids is None or not any(o['id'] == order['order_id'] for o in bids):
                # If there are no bids at this price or there are no bid orders with matching IDs ignore
                return
            # Get the index of the matching orderID in our list of bids 
            index = [b['id'] for b in bids].index(order['order_id'])
            # Update the size of this order
            bids[index]['size'] = new_size
            # Update bid order dict 
            self.setBidsAtThisPrice(price, bids)
        else:
            asks = self.getAsksAtThisPrice(price)
            if asks is None or not any(o['id'] == order['order_id'] for o in asks):
                # If there are no asks at this price or there are no asks orders with matching IDs ignore
                return
            # Get the index of the matching orderID in our list of asks 
            index = [a['id'] for a in asks].index(order['order_id'])
            # Update the size of this order
            asks[index]['size'] = new_size
            # Update ask order dict
            self.setAsksAtThisPrice(price, asks)
        '''
        #implementation to reduce redundancy of if side == buy/sell 
        tree = self._asks if order['side'] == 'sell' else self._bids
        node = tree.get(price)

        if node is None or not any(o['id'] == order['order_id'] for o in node):
            return
        index = [listing['id'] for listing in node].index(order["order_id"])
        node[index]['size'] = new_size
        self.setOrdersAtThisPrice(price,node)
        '''


    # This method returns a list of bid orders at the given price from our bids dict, whose keys are price
    def getBidsAtThisPrice(self,price):
        return self.bids.get(price)
    
    # This method updates our dictionary of bid orders
    def setBidsAtThisPrice(self,price,bidList):
        '''
        This method takes in a list of bids as a parameter, as well as a price.
        It updates/sets the KEY in our bids dict to the given price and associates the VALUE to the passed in bids list
        '''
        self.bids[price] = bidList
    
    # This method removes the bid price from our bids dictionary 
    def removeBidsAtThisPrice(self,price):
        del self.bids[price]

    # This method returns a list of ask orders at the given price from our asks dict, whose keys are price
    def getAsksAtThisPrice(self,price):
        return self.asks.get(price)
    
    # This method updates our dictionary of ask orders
    def setAsksAtThisPrice(self,price,askList):
        '''
        This method takes in a list of asks as a parameter, as well as a price.
        It updates/sets the KEY in our asks dict to the given price and associates the VALUE to the passed in ask list
        ''' 
        self.asks[price] = askList
    
    # This method removes the ask price from our asks dictionary 
    def removeAsksAtThisPrice(self,price):
        del self.asks[price]
    

    # This method returns a list of the top n bid prices
    def getTopBids(self,n):
        # Intialize list to return at the end of method
        topBids = []
        numBids = self.bids.keys().__len__()
        if(n<=numBids):
            # Traverse our sorted bids dict in reverse order as it is sorted in increasing order and we want the highest bid prices
            for i in range(numBids-1,numBids-n-1,-1):
                topBids.append(self.bids.peekitem(i)[0])
        else:
            # If there are fewer bid prices than the number requested (n)
            # Retrieve all the bid prices in bid order book
            for i in range(numBids-1,-1,-1):
                topBids.append(self.bids.peekitem(i)[0])
            # Set the remainder of the top n bids asked for to zero 
            for i in range(n-numBids):
                topBids.append(0.00)
        
        # Return list of top n bids
        return topBids
    
    # This method returns a list of the top n ask prices
    def getTopAsks(self,n):
        # Intialize list to return at the end of method
        topAsks = []
        for i in range(n):
            try:
                topAsks.append(self.asks.peekitem(i)[0])
            except IndexError:
                '''
                peekitem(i) would raise an index error if n were greater than the length of the list of keys in our asks dict. 
                This means there are fewer ask prices than the number requested (n).
                In this case we append a price of inifinity 
                '''
                # topAsks.append(0.00)
                topAsks.append(float('inf'))
        return topAsks




        

