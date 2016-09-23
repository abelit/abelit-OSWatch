from socket import *
from time import ctime

HOST = 'localhost'
PORT = 21560
BUFSIZE = 1024
ADDR = (HOST, PORT)

tcpSerSock = socket(AF_INET, SOCK_STREAM)
tcpSerSock.bind(ADDR)
tcpSerSock.listen(5)
quit = False
shutdown = False

while True:
    print('waiting for connection...')
    tcpCliSock, addr = tcpSerSock.accept()
    print('...connected from: ', addr)

    while True:
        data = tcpCliSock.recv(BUFSIZE)
        data = data.decode('utf8')
        if not data:
            break
        ss = '[%s] %s' %(ctime(), data)
        tcpCliSock.send(ss.encode('utf8'))
        print(ss)
        if data == 'bye':
            quit = True
            break
        elif data == 'shutdown':
            shutdown = True
            break
    print('Bye-bye: [%s: %d]' %(addr[0], addr[1]))
    tcpCliSock.close()

    if shutdown:
        break
tcpSerSock.close()
print('Server has been stoped')