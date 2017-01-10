import tkMessageBox
from Tkinter import *
from ttk import *
from tkFileDialog import askopenfilename
import matplotlib, numpy, sys
import matplotlib.ticker as ticker
import matplotlib.pyplot as pl
from mpl_toolkits.mplot3d import Axes3D


matplotlib.use('TkAgg')
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2TkAgg
from matplotlib.figure import Figure

from DB import *
from ReadPCAP import *

# Global var
db = 0

class Gui(Frame):
    def __init__(self, parent):
        Frame.__init__(self, parent)

        # init root window
        self.parent = parent
        self.parent.title("Network Traffic Analyzer 1.0.0")
        self.pack(fill=BOTH, expand=1)
        self.centerWindow()

        # init menu bar
        self.initMenu()

    def centerWindow(self):
        w = 1000
        h = 600

        sw = self.parent.winfo_screenwidth()
        sh = self.parent.winfo_screenheight()

        x = (sw - w) / 2
        y = (sh - h) / 2
        # self.parent.geometry('%dx%d+%d+%d' % (w, h, x, y))
        self.parent.geometry('%dx%d' % (sw, sh))

    def initMenu(self):
        menu = Menu(root)
        root.config(menu=menu)
        filemenu = Menu(menu)
        menu.add_cascade(label="File", menu=filemenu)
        filemenu.add_command(label="Open PCAP File", command=self.openFile)
        filemenu.add_command(label="Load DB", command=self.loadFile)
        filemenu.add_separator()
        filemenu.add_command(label="Exit", command=root.quit)

        helpmenu = Menu(menu)
        menu.add_cascade(label="Help", menu=helpmenu)
        helpmenu.add_command(label="About", command=self.about)

    def initBars(self):
        note = Notebook(root)

        self.tabCnl = Frame(note)
        self.tabPER = Frame(note)
        self.tabSsn = Frame(note)
        self.tabUsr = Frame(note)
        self.tabCnlPER = Frame(note)

        # add tab and bind to its corresponding graph
        note.add(self.tabCnl, text="Channel")

        note.add(self.tabPER, text="PER")
        self.tabPER.bind("<Button-1>", self.showPacketPerUserGraph())

        note.add(self.tabCnlPER, text="Per By Channel")
        self.tabCnlPER.bind("<Button-1>", self.showPerByChannelGraph())


        # note.add(self.tabCnlPER, text="connctions")
        # self.tabCnlPER.bind("<Button-1>", self.connectionBetwenUsers())

        # self.connectionBetwenUsers()


        note.add(self.tabSsn, text="Sessions")

        note.add(self.tabUsr, text="Users")

        note.place(x=0, y=5)

    def initFrame(self):
        frame = Frame()

    def loadFile(self):
        try:
            fileName = askopenfilename()
            dbName = fileName[fileName.rindex("/") + 1:fileName.rindex(".")]
            global db
            db = DB(dbName)
            print "loaded file"
            self.initBars()
        except:
            pass

    def openFile(self):
        try:
            fileName = askopenfilename()
            labelfont = ('times', 20, 'bold')
            loadingMsg = Label(self.parent, text="Loading...")
            loadingMsg.place(x=450, y=300)
            loadingMsg.config(font=labelfont)
            start = time.time()
            # set DB name and create it
            dbName = fileName[fileName.rindex("/") + 1:fileName.rindex(".")]
            global db
            db = DB(dbName)
            db.createDB()

            # init and start parse
            parser = Parser(fileName)
            parser.startParse()
            # insert to DB
            db.insertToTable("usage", parser.getUsageData())
            db.insertToTable("routers", parser.getRouterData())
            db.insertToTable("session", parser.getSessionData())

            end = time.time()
            loadingMsg.destroy()
            print 'Parsing took %0.3f sec' % (end - start)
            tkMessageBox.showinfo("Database created",
                                  "Done loading data.")
            self.initBars()
        except Exception as inst:
            print type(inst)  # the exception instance

            print inst.args  # arguments stored in .args

            print inst  # __str__ allows args to be printed directly

            print "Error creating DB"
            pass

    def about(self):
        tkMessageBox.showinfo("About", "This is Network Analyzer used to get data about network and network quality.\n"
                                       "Version - 1.0.0")

    def showPacketPerUserGraph(self):
        f = Figure(figsize=(13, 6.5), dpi=100)
        ax = f.add_subplot(111)

        # Get data from database
        global db
        users = db.getUserUsage()

        print (users)
        macs = []
        sent = []
        retransmit = []

        for usr in users:
            macs.append(usr[0])
            sent.append(usr[1])
            retransmit.append(usr[2])

        index = numpy.arange(len(users))  # the x locations for the groups
        bar_width = .1
        opacity = 0.4

        ratioList = []
        for x in range(0,len(retransmit)):
            if (sent[x] + retransmit[x] != 0) :
                ratioList.append((sent[x]/(sent[x]+retransmit[x]))*100)
            else :
                ratioList.append(100)


        ax.bar(index, ratioList, bar_width,
                        alpha=opacity,
                        color='m',
                        label='User Connection Quality')

        # rects2 = ax.bar(index + bar_width, retransmit, bar_width,
        #                 alpha=opacity,
        #                 color='r',
        #                 label='Retransmit')

        ax.legend(loc=1)

        title = "PACKETS PER USER"
        ax.set_title(title)
        ax.set_label("mac addres")
        ax.set_ylabel('Packets PER (%)')
        ax.set_xticks(index)
        ax.set_xticklabels(macs, fontsize='small', ha='right', rotation=20)
        ax.set_xlim([0,20]) #the first 20 macs
        ax.set_ylim([0, 100])


        canvas = FigureCanvasTkAgg(f, master=self.tabPER)

        toolbar = NavigationToolbar2TkAgg(canvas, self.tabPER)
        toolbar.update()
        canvas._tkcanvas.pack(side=RIGHT, fill=BOTH, expand=True)
        canvas.show()

    def showPerByChannelGraph(self):
        f = Figure(figsize=(13, 6.5), dpi=100)
        ax = f.add_subplot(111)

        global db
        channels = db.getChannellUsage()

        channelTab = []
        packets= []
        retransmit= []

        for channel in channels:
            channelTab.append(channel[0])
            packets.append(channel[1])
            retransmit.append(channel[2])

        index = numpy.arange(len(channels))
        bar_width = .1
        opacity = 0.4

        ratioList = []
        for x in range(0,len(retransmit)):
            if (packets[x] + retransmit[x] != 0) :
                ratioList.append((packets[x]/(packets[x]+retransmit[x]) )*100)
            else :
                ratioList.append(100)


        print ratioList

        ax.bar(index, ratioList, bar_width,
                        alpha=opacity,
                        color='y',
                        label='Connection Quality')

        ax.legend(loc=1)

        title = "CHANNEL PER"
        ax.set_title(title)
        ax.set_xlabel('Channels')
        ax.set_ylabel('User PER (%)')
        ax.set_xticks(index)
        ax.set_xticklabels(channelTab, fontsize='small', ha='right', rotation=20)
        ax.set_xlim([0,len(channelTab)])
        ax.set_ylim([0, 100])

        canvas = FigureCanvasTkAgg(f, master=self.tabCnlPER)

        toolbar = NavigationToolbar2TkAgg(canvas, self.tabCnlPER)
        toolbar.update()
        canvas._tkcanvas.pack(side=RIGHT, fill=BOTH, expand=True)
        canvas.show()

    # def connectionBetwenUsers(self):
        # fig = pl.figure()
        # ax = fig.add_subplot(111, projection='3d')
        # n = 100
        #
        # global db
        # connections= db.getConnectionUsers()
        #
        # mac_src=[]
        # mac_dst=[]
        # packets=[]
        # for connect in connections:
        #     mac_src.append(connect[0])
        #     mac_dst.append(connect[1])
        #     packets.append(connect[2])
        #
        # print("connections : " , connections)

        # for c, m, zl, zh in [('r', 'o', -50, -25), ('b', '^', -30, -5)]:
            # xs = randrange(n, 23, 32)
            # ys = randrange(n, 0, 100)
            # zs = randrange(n, zl, zh)
        # ax.scatter("e", "f", 10, 'r', marker='o')

        # ax.set_xlabel('X Label')
        # ax.set_ylabel('Y Label')
        # ax.set_zlabel('Z Label')
        #
        # pl.show()


root = Tk()
ex = Gui(root)

root.mainloop()
