import sys
import time
import datetime as dt
import tkinter
import tkinter.messagebox
import queue 
from OrderBookFull import OrderBookFull
from colors import Colors

'''
The order book gui component and websocket client run on a separate threads. 
It follows a producer-consumer design pattern:
-The gui and websocket threads both share a message queue. 
-The websocket thread PRODUCES the top bid/ask prices and adds it to the shared queue.
-The gui thread CONSUMES the top bid/ask price by removing continuously removing messages from the queue and displaying them.
'''

class OrderBookProducer(OrderBookFull):
        ''' Logs real-time changes to the bid-ask price and sends to gui (consumer) thread '''

        def __init__(self, gui_q,levels,product_id=None):
            super(OrderBookProducer, self).__init__(product_id=product_id)

            # Queue shared by order book thread and gui (consumer) thread that stores order book snapshots
            self.gui_q = gui_q

            # Amount of prices to display in the order book gui
            self.levels = levels
            
        
        def on_message(self, message):
            super(OrderBookProducer, self).on_message(message)

            # Get the current top self.levels asks and bids from the full orderbook 
            # i.e. if self.levels = 5, we get to top 5 bid and ask prices
            topAsks = self.getTopAsks(self.levels)
            topBids = self.getTopBids(self.levels)

            # Construct message to send to gui (receiver) thread
            msgForQ = {"topAsks": topAsks,"topBids":topBids}

            try:
                # Put orderbook snap shot in queue for receiver thread
                self.gui_q.put(msgForQ,block=False)
            except queue.Full:
                # Gui thread has not removed order book snapshot message yet from queue–do not place in another .
                pass

class OrderBookConsumer(tkinter.Frame):
        def __init__(self,parent,in_q,levels):
            tkinter.Frame.__init__(self, parent)
            
            # queue shared by OrderBookConsumer (gui thread) and OrderBookProducer (websocket thread)
            self.in_q = in_q
            
            # Amount of prices to display in the order book gui
            self.levels = levels
            
            # gui window object
            self.parent = parent
            
            # gui window title
            self.parent.title("Order Book")
           
            # This list will hold the stringvars attached to each bid/asl label 
            self.bidTexts = []
            self.askTexts = []

            # This list will hold all the bid/asl labels. Each label is an item in the list displayed in the gui window
            self.bidLabels = []
            self.askLabels = []

            # Initialize the gui window ask list 
            self.createList(self.askLabels,self.askTexts,"red",self.levels)
            
            # Initialize the spread label 
            self.spreadText = tkinter.StringVar()
            self.spreadText.set("0.00")
            self.spreadLabel = tkinter.Label(self.parent,textvariable = self.spreadText,fg = "white",bg="black")
            self.spreadLabel.pack()
            
            # Initialize the gui window bid list 
            self.createList(self.bidLabels,self.bidTexts,"green",self.levels)

            # Start continuous loop to refresh orderbook gui every millisecond
            self.parent.after(1,self.refreshBook)

        # Initialize our bid and ask lists
        def createList(self,labels,texts,textColor,n):
            for i in range(n):
                texts.append(tkinter.StringVar())
                texts[i].set("0.00")
                labels.append(tkinter.Label(self.parent,textvariable = texts[i],fg = textColor,bg="black"))
                labels[i].pack()
        
        # Call this every millisecond to update the order book view 
        def refreshBook(self):
            try:
                # Get (consume) data sent from orderbook thread
                data = self.in_q.get(block=False)
                # Update spread label text
                self.spreadText.set(self.formatPrice(data["topAsks"][0]-data["topBids"][0]))
                # Update ask and bid list text
                self.updateList(data["topAsks"],data["topBids"])
            except queue.Empty:
                # No messages sent from orderbook thread
                pass
            finally:
                # Update order book view again in 1 millisecond
                self.parent.after(1,self.refreshBook)
        
        # This method updates ask and bid lists
        def updateList(self,asks,bids):
            for i,bid in enumerate(bids):
                self.bidTexts[i].set(self.formatPrice(bid))

            for i,ask in enumerate(asks):
                # List asks in reverse order
                self.askTexts[len(self.askTexts)-1-i].set(self.formatPrice(ask))
         
        
        def formatPrice(self,price):
            return '{0:.2f}'.format(price)




class OrderBookGui:

    def __init__(self, levels):
        # Create the gui window
        self.root = tkinter.Tk()
        # Set background to black
        self.root.configure(bg='black')

        # Create queue shared by OrderBookProducer (websocket thread) OrderBookConsumer (gui thread).
        self.q = queue.Queue(maxsize=1)

        # The amount of the bid/ask prices to display
        self.levels = levels
        
        # Create OrderbookConsumer 
        self.receiver = OrderBookConsumer(self.root,self.q,self.levels)
        # Create OrderbookProducer
        self.producer = OrderBookProducer(self.q,self.levels)
       
        # Start webscoket (producer) thread
        self.producer.start()

        
        # Binds close window button to askTerminate
        self.root.protocol("WM_DELETE_WINDOW", self.askTerminate)
        
        # Start Gui loop (consumer thread)
        self.root.mainloop()
   
    # Function is called when user closes GUI window
    def askTerminate(self):
        if tkinter.messagebox.askokcancel("Quit", "You want to close the order book?"):
            self.root.destroy()
            self.producer.close()

    
    