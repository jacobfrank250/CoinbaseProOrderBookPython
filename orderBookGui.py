import sys
import time
import datetime as dt
import tkinter
import tkinter.messagebox
import queue 
from OrderBookFull import OrderBookFull
from colors import Colors

class OrderBookProducer(OrderBookFull):
        ''' Logs real-time changes to the bid-ask price and sends to receiver thread '''

        def __init__(self, gui_q,levels,product_id=None):
            super(OrderBookProducer, self).__init__(product_id=product_id)

            # queue shared by orderbook thread and gui (receiver) thread that stores orderbook snapshots
            self.gui_q = gui_q

            # amount of prices to display in the orderbook snapshot gui
            self.levels = levels
            
        
        def on_message(self, message):
            super(OrderBookProducer, self).on_message(message)

            # Get the current top self.levels asks and bids from the full orderbook  
            topAsks = self.getTopAsks(self.levels)
            topBids = self.getTopBids(self.levels)

            # construct message to send to receiver thread
            msgForQ = {"topAsks": topAsks,"topBids":topBids}

            try:
                # Put orderbook snap shot in queue for receiver thread
                self.gui_q.put(msgForQ,block=False)
            except queue.Full:
                # receiver thread has not removed orderbook snap shot message yet from queue; do not place another in.
                pass

class OrderBookReceiver(tkinter.Frame):
        def __init__(self,parent,in_q,levels):
            tkinter.Frame.__init__(self, parent)

            self.in_q = in_q
            self.levels = levels
            self.parent = parent
            self.parent.title("Order Book")
           

            self.bidTexts = []
            self.bidLabels = []
            self.askTexts = []
            self.askLabels = []

            
            self.createList(self.askLabels,self.askTexts,"red",self.levels)
            
            self.spreadText = tkinter.StringVar()
            self.spreadText.set("0.00")
            self.spreadLabel = tkinter.Label(self.parent,textvariable = self.spreadText,fg = "white",bg="black")
            self.spreadLabel.pack()
            self.createList(self.bidLabels,self.bidTexts,"green",self.levels)

            # start continues loop to refresh orderbook gui every millisecond
            self.parent.after(1,self.refreshBook)

        # Initialized our bid and ask lists
        def createList(self,labels,texts,textColor,n):
            for i in range(n):
                texts.append(tkinter.StringVar())
                texts[i].set("0.00")
                labels.append(tkinter.Label(self.parent,textvariable = texts[i],fg = textColor,bg="black"))
                labels[i].pack()
        
        # Call this every millisecond to update the order book view 
        def refreshBook(self):
            try:
                #get data sent from orderbook thread
                data = self.in_q.get(block=False)
                #update spread label text
                self.spreadText.set(self.formatPrice(data["topAsks"][0]-data["topBids"][0]))
                #update ask and bid list text
                self.updateList(data["topAsks"],data["topBids"])
            except queue.Empty:
                # No messages sent from orderbook thread
                pass
            finally:
                #update orderbook view in 1 millisecond
                self.parent.after(1,self.refreshBook)
        
        # This method updates ask and bid lists
        def updateList(self,asks,bids):
            for i,bid in enumerate(bids):
                self.bidTexts[i].set(self.formatPrice(bid))

            for i,ask in enumerate(asks):
                # List Asks in reverse order
                self.askTexts[len(self.askTexts)-1-i].set(self.formatPrice(ask))
         
        
        def formatPrice(self,price):
            return '{0:.2f}'.format(price)




class OrderBookGui:

    def __init__(self, levels):
        self.root = tkinter.Tk()
        self.root.configure(bg='black')
        self.q = queue.Queue(maxsize=1)
        self.levels = levels
        self.receiver = OrderBookReceiver(self.root,self.q,self.levels)
        self.producer = OrderBookProducer(self.q,self.levels)
        self.producer.start()

        
        
        self.root.protocol("WM_DELETE_WINDOW", self.askTerminate)
        self.root.mainloop()
   
    # Function is called when user closes GUI window
    def askTerminate(self):
        if tkinter.messagebox.askokcancel("Quit", "You want to close the order book?"):
            self.root.destroy()
            self.producer.close()

    
    