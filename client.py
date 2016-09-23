from socket import *  
  
HOST = 'localhost'  
PORT = 21560
BUFSIZE = 1024  
ADDR = (HOST, PORT)  
  
tcpCliSock = socket(AF_INET, SOCK_STREAM)  
tcpCliSock.connect(ADDR)  
  
while True:  
    data = input('>')  
    if not data:  
        continue  
    print('input data: [%s]' %data)  
    tcpCliSock.send(data.encode('utf8'))  
    rdata = tcpCliSock.recv(BUFSIZE)  
    if not rdata:  
        break  
    print(rdata.decode('utf8'))  
    if data == 'bye' or data == 'shutdown':  
        break  
  
tcpCliSock.close()  