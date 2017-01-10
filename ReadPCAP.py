from scapy.all import *


class Parser:
    usage = {}
    session = {}
    router = {}

    def __init__(self, fileName):
        self.pcapFile = fileName

    def startParse(self):
        print ("Strt parsing the data...")
        packets = rdpcap(self.pcapFile)

        for packet in packets:
            packetString = str(packet[1].__repr__())
            # Subtype(1): src, dest, retry, write in usage and session (channel 0).
            if packetString[packetString.index("subtype=") + 8:packetString.index("L type=")] == "1":
                src = packetString[packetString.index("addr2=") + 6:packetString.index("addr3=") - 1]
                dest = packetString[packetString.index("addr1=") + 6:packetString.index("addr2=") - 1]
                retry = packetString[packetString.index("FCfield=") + 8:packetString.index("ID=") - 1]

                # Insert to Usage table
                usageKey = src
                if self.usage.__contains__(usageKey):
                    valueList = self.usage.get(usageKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.usage[usageKey] = valueList
                else:
                    self.usage[usageKey] = [1, 0]

                # Insert to Session table
                sessionKey = src + "," + dest + ",0"
                if self.session.__contains__(sessionKey):
                    valueList = self.session.get(sessionKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.session[sessionKey] = valueList
                else:
                    self.session[sessionKey] = [1, 0]

            # Subtype(5): src, dest, retry, channel write in usage and session.
            elif packetString[packetString.index("subtype=") + 8:packetString.index("L type=")] == "5":
                src = packetString[packetString.index("addr2=") + 6:packetString.index("addr3=") - 1]
                dest = packetString[packetString.index("addr1=") + 6:packetString.index("addr2=") - 1]
                retry = packetString[packetString.index("FCfield=") + 8:packetString.index("ID=") - 1]
                channel = packetString[packetString.index("DSset") + 21:packetString.index("DSset") + 22]
                if channel == "a":
                    channel = "10"
                elif channel == "b":
                    channel = "11"
                elif channel == "c":
                    channel = "12"
                elif channel == "d":
                    channel = "13"

                # Insert to Usage table
                usageKey = src
                if self.usage.__contains__(usageKey):
                    valueList = self.usage.get(usageKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.usage[usageKey] = valueList
                else:
                    self.usage[usageKey] = [1, 0]

                # Insert to Session table
                sessionKey = src + "," + dest + "," + channel
                if self.session.__contains__(sessionKey):
                    valueList = self.session.get(sessionKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.session[sessionKey] = valueList
                else:
                    self.session[sessionKey] = [1, 0]

            # Subtype(2): src, dest, retry, write in usage and session (channel 0)
            elif packetString[packetString.index("subtype=") + 8:packetString.index("L type=")] == "2":
                src = packetString[packetString.index("addr2=") + 6:packetString.index("addr3=") - 1]
                dest = packetString[packetString.index("addr1=") + 6:packetString.index("addr2=") - 1]
                retry = packetString[packetString.index("FCfield=") + 8:packetString.index("ID=") - 1]
                # Insert to Usage table
                usageKey = src
                if self.usage.__contains__(usageKey):
                    valueList = self.usage.get(usageKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.usage[usageKey] = valueList
                else:
                    self.usage[usageKey] = [1, 0]

                # Insert to Session table
                sessionKey = src + "," + dest + ",0"
                if self.session.__contains__(sessionKey):
                    valueList = self.session.get(sessionKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.session[sessionKey] = valueList
                else:
                    self.session[sessionKey] = [1, 0]

            # Subtype(8): Control / Data / Management.
            # Cotrol dest without src and channel, session only (channel 0).
            elif packetString[packetString.index("subtype=") + 8:packetString.index("L type=")] == "8" and packetString[
                                                                                                           packetString.index(
                                                                                                                   "L type=") + 7:packetString.index(
                                                                                                                   "proto=") - 1] == "Control":
                dest = packetString[packetString.index("addr1=") + 6:packetString.index("addr2=") - 1]
                retry = packetString[packetString.index("FCfield=") + 8:packetString.index("ID=") - 1]
                # Insert to Session table
                sessionKey = "NULL," + dest + ",0"
                if self.session.__contains__(sessionKey):
                    valueList = self.session.get(sessionKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.session[sessionKey] = valueList
                else:
                    self.session[sessionKey] = [1, 0]

            # Data src, dest, write in usage and session (channel 0).
            elif packetString[packetString.index("subtype=") + 8:packetString.index("L type=")] == "8" and packetString[
                                                                                                           packetString.index(
                                                                                                                   "L type=") + 7:packetString.index(
                                                                                                                   "proto=") - 1] == "Data":
                src = packetString[packetString.index("addr2=") + 6:packetString.index("addr3=") - 1]
                dest = packetString[packetString.index("addr1=") + 6:packetString.index("addr2=") - 1]
                retry = packetString[packetString.index("FCfield=") + 8:packetString.index("ID=") - 1]

                # Insert to Usage table
                usageKey = src
                if self.usage.__contains__(usageKey):
                    valueList = self.usage.get(usageKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.usage[usageKey] = valueList
                else:
                    self.usage[usageKey] = [1, 0]

                # Insert to Session table
                sessionKey = src + "," + dest + ",0"
                if self.session.__contains__(sessionKey):
                    valueList = self.session.get(sessionKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.session[sessionKey] = valueList
                else:
                    self.session[sessionKey] = [1, 0]

            # Management src, dest, SSID, channel, write usage, session and router.
            elif packetString[packetString.index("subtype=") + 8:packetString.index("L type=")] == "8" and packetString[
                                                                                                           packetString.index(
                                                                                                                   "L type=") + 7:packetString.index(
                                                                                                                   "proto=") - 1] == "Management":
                src = packetString[packetString.index("addr2=") + 6:packetString.index("addr3=") - 1]
                dest = packetString[packetString.index("addr1=") + 6:packetString.index("addr2=") - 1]
                retry = packetString[packetString.index("FCfield=") + 8:packetString.index("ID=") - 1]
                channel = packetString[packetString.index("DSset") + 21:packetString.index("DSset") + 22]
                if channel == "a":
                    channel = "10"
                elif channel == "b":
                    channel = "11"
                elif channel == "c":
                    channel = "12"
                elif channel == "d":
                    channel = "13"
                SSIDString = str(packet[3].__repr__())
                SSID = SSIDString[SSIDString.find("info='") + 6:SSIDString.find("' |")]

                # Insert to Usage table
                usageKey = src
                if self.usage.__contains__(usageKey):
                    valueList = self.usage.get(usageKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.usage[usageKey] = valueList
                else:
                    self.usage[usageKey] = [1, 0]

                # Insert to Session table
                sessionKey = src + "," + dest + "," + channel
                if self.session.__contains__(sessionKey):
                    valueList = self.session.get(sessionKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.session[sessionKey] = valueList
                else:
                    self.session[sessionKey] = [1, 0]

                # Insert to Router table
                routerKey = src + "," + channel
                if self.router.__contains__(routerKey) == False:
                    self.router[routerKey] = [SSID, 0]

            # Subtype(13): Control / Management.
            # Cotrol dest witout src and channel, session only (channel 0).
            elif packetString[
                 packetString.index("subtype=") + 8:packetString.index("L type=")] == "13" and packetString[
                                                                                               packetString.index(
                                                                                                       "L type=") + 7:packetString.index(
                                                                                                       "proto=") - 1] == "Control":
                dest = packetString[packetString.index("addr1=") + 6:packetString.index("addr1=") + 23]
                retry = packetString[packetString.index("FCfield=") + 8:packetString.index("ID=") - 1]
                # Insert to Session table
                sessionKey = "NULL," + dest + ",0"
                if self.session.__contains__(sessionKey):
                    valueList = self.session.get(sessionKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.session[sessionKey] = valueList
                else:
                    self.session[sessionKey] = [1, 0]

            # Management src, dest, write in usgae and session (channel 0).
            elif packetString[
                 packetString.index("subtype=") + 8:packetString.index("L type=")] == "13" and packetString[
                                                                                               packetString.index(
                                                                                                       "L type=") + 7:packetString.index(
                                                                                                       "proto=") - 1] == "Management":
                src = packetString[packetString.index("addr2=") + 6:packetString.index("addr3=") - 1]
                dest = packetString[packetString.index("addr1=") + 6:packetString.index("addr2=") - 1]
                retry = packetString[packetString.index("FCfield=") + 8:packetString.index("ID=") - 1]

                # Insert to Usage table
                usageKey = src
                if self.usage.__contains__(usageKey):
                    valueList = self.usage.get(usageKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.usage[usageKey] = valueList
                else:
                    self.usage[usageKey] = [1, 0]

                # Insert to Session table
                sessionKey = src + "," + dest + ",0"
                if self.session.__contains__(sessionKey):
                    valueList = self.session.get(sessionKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.session[sessionKey] = valueList
                else:
                    self.session[sessionKey] = [1, 0]

            # Subtype(0): Management / Data.
            # Management src, dest usage and session (channel 0).
            elif packetString[packetString.index("subtype=") + 8:packetString.index("L type=")] == "0" and packetString[
                                                                                                           packetString.index(
                                                                                                                   "L type=") + 7:packetString.index(
                                                                                                                   "proto=") - 1] == "Management":
                src = packetString[packetString.index("addr2=") + 6:packetString.index("addr3=") - 1]
                dest = packetString[packetString.index("addr1=") + 6:packetString.index("addr2=") - 1]
                retry = packetString[packetString.index("FCfield=") + 8:packetString.index("ID=") - 1]

                # Insert to Usage table
                usageKey = src
                if self.usage.__contains__(usageKey):
                    valueList = self.usage.get(usageKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.usage[usageKey] = valueList
                else:
                    self.usage[usageKey] = [1, 0]

                # Insert to Session table
                sessionKey = src + "," + dest + ",0"
                if self.session.__contains__(sessionKey):
                    valueList = self.session.get(sessionKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.session[sessionKey] = valueList
                else:
                    self.session[sessionKey] = [1, 0]

            # Data src, dest usage and session (channel 0).
            elif packetString[packetString.index("subtype=") + 8:packetString.index("L type=")] == "0" and packetString[
                                                                                                           packetString.index(
                                                                                                                   "L type=") + 7:packetString.index(
                                                                                                                   "proto=") - 1] == "Data":
                src = packetString[packetString.index("addr2=") + 6:packetString.index("addr3=") - 1]
                dest = packetString[packetString.index("addr1=") + 6:packetString.index("addr2=") - 1]
                retry = packetString[packetString.index("FCfield=") + 8:packetString.index("ID=") - 1]

                # Insert to Usage table
                usageKey = src
                if self.usage.__contains__(usageKey):
                    valueList = self.usage.get(usageKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.usage[usageKey] = valueList
                else:
                    self.usage[usageKey] = [1, 0]

                # Insert to Session table
                sessionKey = src + "," + dest + ",0"
                if self.session.__contains__(sessionKey):
                    valueList = self.session.get(sessionKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.session[sessionKey] = valueList
                else:
                    self.session[sessionKey] = [1, 0]

            # Subtype(11): Management / Control.
            # Management src, dest usage and session (channel 0).
            elif packetString[
                 packetString.index("subtype=") + 8:packetString.index("L type=")] == "11" and packetString[
                                                                                               packetString.index(
                                                                                                       "L type=") + 7:packetString.index(
                                                                                                       "proto=") - 1] == "Management":
                src = packetString[packetString.index("addr2=") + 6:packetString.index("addr3=") - 1]
                dest = packetString[packetString.index("addr1=") + 6:packetString.index("addr2=") - 1]
                retry = packetString[packetString.index("FCfield=") + 8:packetString.index("ID=") - 1]

                # Insert to Usage table
                usageKey = src
                if self.usage.__contains__(usageKey):
                    valueList = self.usage.get(usageKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.usage[usageKey] = valueList
                else:
                    self.usage[usageKey] = [1, 0]

                # Insert to Session table
                sessionKey = src + "," + dest + ",0"
                if self.session.__contains__(sessionKey):
                    valueList = self.session.get(sessionKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.session[sessionKey] = valueList
                else:
                    self.session[sessionKey] = [1, 0]

            # Cotrol src, dest, usage and session (channel 0).
            elif packetString[
                 packetString.index("subtype=") + 8:packetString.index("L type=")] == "11" and packetString[
                                                                                               packetString.index(
                                                                                                       "L type=") + 7:packetString.index(
                                                                                                       "proto=") - 1] == "Control":
                src = packetString[packetString.index("addr2=") + 6:packetString.index("addr2=") + 23]
                dest = packetString[packetString.index("addr1=") + 6:packetString.index("addr1=") + 23]
                retry = packetString[packetString.index("FCfield=") + 8:packetString.index("ID=") - 1]

                # Insert to Usage table
                usageKey = src
                if self.usage.__contains__(usageKey):
                    valueList = self.usage.get(usageKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.usage[usageKey] = valueList
                else:
                    self.usage[usageKey] = [1, 0]

                # Insert to Session table
                sessionKey = src + "," + dest + ",0"
                if self.session.__contains__(sessionKey):
                    valueList = self.session.get(sessionKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.session[sessionKey] = valueList
                else:
                    self.session[sessionKey] = [1, 0]

            # Subtype(12): Control / Management / Data.
            # Control dest, session only (channel 0).
            elif packetString[
                 packetString.index("subtype=") + 8:packetString.index("L type=")] == "12" and packetString[
                                                                                               packetString.index(
                                                                                                       "L type=") + 7:packetString.index(
                                                                                                       "proto=") - 1] == "Control":
                # dest = packetString[packetString.index("addr1=") + 6:packetString.index("addr2=")-1]
                dest = packetString[packetString.index("addr1=") + 6:packetString.index("addr1=") + 23]
                retry = packetString[packetString.index("FCfield=") + 8:packetString.index("ID=") - 1]

                # Insert to Session table
                sessionKey = "NULL," + dest + ",0"
                if self.session.__contains__(sessionKey):
                    valueList = self.session.get(sessionKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.session[sessionKey] = valueList
                else:
                    self.session[sessionKey] = [1, 0]

            # Management src, dest, usage and session (channel 0).
            elif packetString[
                 packetString.index("subtype=") + 8:packetString.index("L type=")] == "12" and packetString[
                                                                                               packetString.index(
                                                                                                       "L type=") + 7:packetString.index(
                                                                                                       "proto=") - 1] == "Management":
                src = packetString[packetString.index("addr2=") + 6:packetString.index("addr3=") - 1]
                dest = packetString[packetString.index("addr1=") + 6:packetString.index("addr2=") - 1]
                retry = packetString[packetString.index("FCfield=") + 8:packetString.index("ID=") - 1]

                # Insert to Usage table
                usageKey = src
                if self.usage.__contains__(usageKey):
                    valueList = self.usage.get(usageKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.usage[usageKey] = valueList
                else:
                    self.usage[usageKey] = [1, 0]

                # Insert to Session table
                sessionKey = src + "," + dest + ",0"
                if self.session.__contains__(sessionKey):
                    valueList = self.session.get(sessionKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.session[sessionKey] = valueList
                else:
                    self.session[sessionKey] = [1, 0]

            # Data src, dest, usage and session (channel 0).
            elif packetString[
                 packetString.index("subtype=") + 8:packetString.index("L type=")] == "12" and packetString[
                                                                                               packetString.index(
                                                                                                       "L type=") + 7:packetString.index(
                                                                                                       "proto=") - 1] == "Data":
                src = packetString[packetString.index("addr2=") + 6:packetString.index("addr3=") - 1]
                dest = packetString[packetString.index("addr1=") + 6:packetString.index("addr2=") - 1]
                retry = packetString[packetString.index("FCfield=") + 8:packetString.index("ID=") - 1]

                # Insert to Usage table
                usageKey = src
                if self.usage.__contains__(usageKey):
                    valueList = self.usage.get(usageKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.usage[usageKey] = valueList
                else:
                    self.usage[usageKey] = [1, 0]

                # Insert to Session table
                sessionKey = src + "," + dest + ",0"
                if self.session.__contains__(sessionKey):
                    valueList = self.session.get(sessionKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.session[sessionKey] = valueList
                else:
                    self.session[sessionKey] = [1, 0]

            # Subtype(4): Data / Management.
            # Control src, dest, usage and session (channel 0).
            elif packetString[packetString.index("subtype=") + 8:packetString.index("L type=")] == "4" and packetString[
                                                                                                           packetString.index(
                                                                                                                   "L type=") + 7:packetString.index(
                                                                                                                   "proto=") - 1] == "Data":
                src = packetString[packetString.index("addr2=") + 6:packetString.index("addr3=") - 1]
                dest = packetString[packetString.index("addr1=") + 6:packetString.index("addr2=") - 1]
                retry = packetString[packetString.index("FCfield=") + 8:packetString.index("ID=") - 1]

                # Insert to Usage table
                usageKey = src
                if self.usage.__contains__(usageKey):
                    valueList = self.usage.get(usageKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.usage[usageKey] = valueList
                else:
                    self.usage[usageKey] = [1, 0]

                # Insert to Session table
                sessionKey = src + "," + dest + ",0"
                if self.session.__contains__(sessionKey):
                    valueList = self.session.get(sessionKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.session[sessionKey] = valueList
                else:
                    self.session[sessionKey] = [1, 0]

            # Management src, dest, channel, usage and session (channel 0).
            elif packetString[packetString.index("subtype=") + 8:packetString.index("L type=")] == "4" and packetString[
                                                                                                           packetString.index(
                                                                                                                   "L type=") + 7:packetString.index(
                                                                                                                   "proto=") - 1] == "Management":
                src = packetString[packetString.index("addr2=") + 6:packetString.index("addr3=") - 1]
                dest = packetString[packetString.index("addr1=") + 6:packetString.index("addr2=") - 1]
                retry = packetString[packetString.index("FCfield=") + 8:packetString.index("ID=") - 1]
                if packetString.find("DSset") == -1:
                    channel = "0"
                else:
                    channel = packetString[packetString.index("DSset") + 21:packetString.index("DSset") + 22]
                    if channel == "a":
                        channel = "10"
                    elif channel == "b":
                        channel = "11"
                    elif channel == "c":
                        channel = "12"
                    elif channel == "d":
                        channel = "13"
                    elif channel == " ":
                        channel = "0"

                # Insert to Usage table
                usageKey = src
                if self.usage.__contains__(usageKey):
                    valueList = self.usage.get(usageKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.usage[usageKey] = valueList
                else:
                    self.usage[usageKey] = [1, 0]

                # Insert to Session table
                sessionKey = src + "," + dest + "," + channel
                if self.session.__contains__(sessionKey):
                    valueList = self.session.get(sessionKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.session[sessionKey] = valueList
                else:
                    self.session[sessionKey] = [1, 0]

            # Subtype(10): Control / Management.
            # Control dest, session only (channel 0).
            elif packetString[
                 packetString.index("subtype=") + 8:packetString.index("L type=")] == "10" and packetString[
                                                                                               packetString.index(
                                                                                                       "L type=") + 7:packetString.index(
                                                                                                       "proto=") - 1] == "Control":
                dest = packetString[packetString.index("addr1=") + 6:packetString.index("addr2=") - 1]
                retry = packetString[packetString.index("FCfield=") + 8:packetString.index("ID=") - 1]

                # Insert to Session table
                sessionKey = "NULL," + dest + ",0"
                if self.session.__contains__(sessionKey):
                    valueList = self.session.get(sessionKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.session[sessionKey] = valueList
                else:
                    self.session[sessionKey] = [1, 0]

            # Management src, dest, usage and session (channel 0).
            elif packetString[
                 packetString.index("subtype=") + 8:packetString.index("L type=")] == "10" and packetString[
                                                                                               packetString.index(
                                                                                                       "L type=") + 7:packetString.index(
                                                                                                       "proto=") - 1] == "Management":
                src = packetString[packetString.index("addr2=") + 6:packetString.index("addr3=") - 1]
                dest = packetString[packetString.index("addr1=") + 6:packetString.index("addr2=") - 1]
                retry = packetString[packetString.index("FCfield=") + 8:packetString.index("ID=") - 1]

                # Insert to Usage table
                usageKey = src
                if self.usage.__contains__(usageKey):
                    valueList = self.usage.get(usageKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.usage[usageKey] = valueList
                else:
                    self.usage[usageKey] = [1, 0]

                # Insert to Session table
                sessionKey = src + "," + dest + ",0"
                if self.session.__contains__(sessionKey):
                    valueList = self.session.get(sessionKey)
                    valueList[0] += 1
                    if retry == "retry":
                        valueList[1] += 1
                    self.session[sessionKey] = valueList
                else:
                    self.session[sessionKey] = [1, 0]
        self.getRouterConnections()

    def priUsageDic(self):
        for usageKey in self.usage.iterkeys():
            print usageKey + " = " + str(self.usage.get(usageKey))

    def priSessionDic(self):
        for sessionKey in self.session.iterkeys():
            print sessionKey + " = " + str(self.session.get(sessionKey))

    def priRouterDic(self):
        for routerKey in self.router.iterkeys():
            print routerKey + " = " + str(self.router.get(routerKey))

    def getRouterConnections(self):
        for router in self.router.iterkeys():
            macConnectionList = []
            for packet in self.session.iterkeys():
                if (packet[:17] + packet[35:] == router):
                    if packet[18:] not in macConnectionList and packet[18:35] != "ff:ff:ff:ff:ff:ff":
                        self.router.get(router)[1] += 1
                        macConnectionList.append(packet[18:])

    def getUsageData(self):
        return self.usage

    def getSessionData(self):
        return self.session

    def getRouterData(self):
        return self.router
