import os
import sqlite3


class DB:
    def __init__(self, DataBaseName):
        self.dbName = DataBaseName + ".db"

    def createDB(self):
        # check if DB exsist -> if so delete
        if(os.path.isfile(self.dbName)):
            os.remove(self.dbName)
        # connect to DB
        connection = sqlite3.connect(self.dbName)

        cursor = connection.cursor()

        # create Usgae table
        create_usage_table = """
        CREATE TABLE usage (
        MAC VARCHAR(17) PRIMARY KEY,
        sent DOUBLE,
        retransmit DOUBLE);"""
        cursor.execute(create_usage_table)

        # create Session table
        create_session_table = """
        CREATE TABLE session (
        MAC_SRC VARCHAR(17),
        MAC_DST VARCHAR(17),
        channel INT,
        packets DOUBLE,
        retransmit DOUBLE,
        PRIMARY KEY (MAC_SRC, MAC_DST, channel));"""
        cursor.execute(create_session_table)

        # create Routers table
        create_routers_table = """
        CREATE TABLE routers (
        ROUTER VARCHAR(17),
        CHANNEL INT,
        SSID VARCHAR(256),
        CONNECTIONS INTEGER,
        PRIMARY KEY (ROUTER, CHANNEL));"""
        cursor.execute(create_routers_table)

        # commit commands and close connection to DB
        connection.commit()
        connection.close()
        print("finish creating DB " + self.dbName)

    def insertToTable(self, tableName, data):

        if (tableName == "usage"):
            self.__insertToUsageTable(data)
        elif (tableName == "session"):
            self.__insertToSessionTable(data)
        elif (tableName == "routers"):
            self.__insertToRoutersTable(data)
        else:
            print("ERROR: No such table - " + tableName)

    def __insertToUsageTable(self, data):

        # connect to db
        connection = sqlite3.connect(self.dbName)
        cursor = connection.cursor()

        for pkts in data.iterkeys():
            format_str = """INSERT INTO usage (MAC, sent, retransmit)
            VALUES ("{mac}", "{sent}", "{retransmit}");"""

            sql_command = format_str.format(mac=pkts, sent=data.get(pkts)[0], retransmit=data.get(pkts)[1])
            cursor.execute(sql_command)

        connection.commit()
        connection.close()

        print("Done insert to usage table")

    def __insertToSessionTable(self, data):
        # connect to db
        connection = sqlite3.connect(self.dbName)
        cursor = connection.cursor()

        broadcastNum = 0
        mac_dst = ""
        for pkts in data.iterkeys():
            format_str = """INSERT INTO session (MAC_SRC, MAC_DST ,packets, retransmit, channel)
                    VALUES ("{mac_src}", "{mac_dst}", "{packets}", "{retransmit}", "{channel}");"""
            # if (pkts[18:35] == "ff:ff:ff:ff:ff:ff"):
            #     sql_command = format_str.format(mac_src=pkts[:17], mac_dst = broadcastNum, packets=data.get(pkts)[0],
            #                                     retransmit=data.get(pkts)[1], channel=pkts[36:])
            #     broadcastNum += 1
            # else:
            sql_command = format_str.format(mac_src=pkts[:17], mac_dst=pkts[18:35], packets=data.get(pkts)[0],
                                                retransmit=data.get(pkts)[1], channel=pkts[36:])

            cursor.execute(sql_command)

        connection.commit()
        connection.close()

        print("Done insert to session table")

    def __insertToRoutersTable(self, data):
        # connect to db
        connection = sqlite3.connect(self.dbName)
        cursor = connection.cursor()

        for pkts in data.iterkeys():
            format_str = """INSERT INTO routers (ROUTER, CHANNEL ,SSID, CONNECTIONS)
                            VALUES ("{router}", "{channel}", "{ssid}", "{connections}");"""

            sql_command = format_str.format(router=pkts[:17], channel=pkts[18:], ssid=data.get(pkts)[0],
                                            connections=data.get(pkts)[1])
            cursor.execute(sql_command)

        connection.commit()
        connection.close()

        print("Done insert to routers table")


    # ******************
    # ***   Quries   ***
    # ******************

    # run the query at the database and return result
    def __getQuery(self, query):
        connection = sqlite3.connect(self.dbName)
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        connection.close()
        return result

    # return each user usage
    # (MAC, packets, retransmit packets)
    def getUserUsage(self):
        return self.__getQuery("Select * from usage")

    def getChannellUsage(self):
        return self.__getQuery("select channel, sum(packets), sum(retransmit) from session group by channel")

    def getConnectionUsers(self):
        return self.__getQuery("select MAC_SRC, MAC_DST, packets from session where  (channel != '') "
                               " and (MAC_DST != 'ff:ff:ff:ff:ff:ff') ")
    # def getUsersConnection(self):
    #     return self.__getQuery("se")

    def getchannelEfficiency(self):
        return self.__getQuery("select SSID, sum(CONNECTIONS) from routers where CONNECTIONS != '' group by SSID")

    def sesseionsNum(self):
        return self.__getQuery("select MAC_SRC,count(MAC_SRC) from session where  (channel != '') group by MAC_SRC")
 # M

'''
#DB run example

rtrs = dict()
sses = dict()
usage= dict()

rtrs["FF:FF:FF:FF:FF:FF,4"] = ["toto", 2]
rtrs["TO:ME:RG:AY:GA:DL,11"] = ["toto2", 3]
rtrs["TO:ME:RB:LE:AA:AA,3"] = ["toto3", 11]

sses["FF:FF:FF:FF:FF:FF,TT:TT:TT:TT:TT:TT,1"] = [525,72]
sses["HH:HH:HH:HH:HH:HH,DD:DD:DD:DD:DD:DD,2"] = [285,72]
sses["GG:GG:GG:Gg:GG:GG,YY:YY:YY:YY:YY:YY,3"] = [16,1]

usage["FF:FF:FF:FF:FF:FF"] = [55,7]
usage["HH:HH:HH:HH:HH:HH"] = [25,2]
usage["GG:GG:GG:GG:GG:GG"] = [156,1]

dataTest = DB("test")
dataTest.createDB()

dataTest.insertToTable("routers", rtrs)
dataTest.insertToTable("session", sses)
dataTest.insertToTable("usage", usage)

'''








