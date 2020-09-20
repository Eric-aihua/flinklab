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
            for i in range(10):
                s = random.choice(['a','b','c','d'])
                word = "%s\n" %"".join(s)
                c.send(word.encode("utf-8"))
                print(word)
            time.sleep(1)

        c.close()

if __name__ == "__main__":
    main()