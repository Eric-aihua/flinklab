#!/usr/bin/python
import random

import socket
import time

server = socket.socket()
server.bind(('127.0.0.1',7777))
server.listen(5)
def main():
    while True:
        c,addr = server.accept()
        print("received a request!")
        while True:
            for i in range(100):
                s = random.sample('zyxwvu',3)
                word = "".join(s)
                time_str= '%s,%s\n' %(word,str(int(time.time())*1000))
                c.send(time_str.encode("utf-8"))
                print(time_str)
            time.sleep(1)

        c.close()

if __name__ == "__main__":
    main()