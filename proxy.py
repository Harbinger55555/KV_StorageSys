"""A proxy server that forwards requests from one port to another server.

To run this using Python 2.7:

% python proxy.py

It listens on a port (`LISTENING_PORT`, below) and forwards commands to the
server. The server is at `SERVER_ADDRESS`:`SERVER_PORT` below.
"""

# This code uses Python 2.7. These imports make the 2.7 code feel a lot closer
# to Python 3. (They're also good changes to the language!)
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import library
import socket

# Where to find the server. This assumes it's running on the smae machine
# as the proxy, but on a different port.
SERVER_ADDRESS = 'localhost'
SERVER_PORT = 7777

# The port that the proxy server is going to occupy. This could be the same
# as SERVER_PORT, but then you couldn't run the proxy and the server on the
# same machine.
LISTENING_PORT = 8888

# Cache values retrieved from the server for this long.
MAX_CACHE_AGE_SEC = 60.0  # 1 minute


def ForwardCommandToServer(msg_to_server):
    """Opens a TCP socket to the server, sends a command, and returns response.

    Args:
      msg_to_server: Contains the cmdline, server_addr, and server_port
    Returns:
      A single line string response with no newlines.
    """
    command_line, server_addr, server_port = msg_to_server
    
    # Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    server_address = (server_addr, server_port)
    sock.connect(server_address)
    res = None

    # TODO: currently sending whole cmdline, check if only cmd is sent in real
    # protocol.
    try:
        # Relay command_line to server and return the response.
        sock.sendall(command_line.encode())
        res = library.ReadCommand(sock).strip('\n')

    finally:
        sock.close()
    
    return res
  

def PutCommand(name, text, cache, msg_to_server):
    """Handle the PUT command for a server.

    PUT's first argument is the name of the key to store the value under.
    All remaining arguments are stitched together and used as the value.

    Args:
        name: The name of the value to store.
        text: The value to store.
        cache: A KeyValueStore containing key/value pairs.
        msg_to_server: Contains the cmdline, server_addr, and server_port
    Returns:
        A human readable string describing the result. If there is an error,
        then the string describes the error.
    """

    # Store the value in the cache then relay the PUT to main server.
    cache.StoreValue(name, text)
    return ForwardCommandToServer(msg_to_server)


def GetCommand(name, cache, msg_to_server):
    """Handle the GET command for a server.

    GET takes a single argument: the name of the key to look up.

    Args:
      name: The name of the value to retrieve.
      cache: A KeyValueStore containing key/value pairs.
    Returns:
      A human readable string describing the result. If there is an error,
      then the string describes the error.
    """
    # If name exists in cache, send value to client immediately. Else,
    # relay to main server and return response.
    res = cache.GetValue(name, MAX_CACHE_AGE_SEC)
    if not res:
        res = ForwardCommandToServer(msg_to_server)
        # Update cache if name existed in main server database.
        if (res != "Key does not exist!"):
            cache.StoreValue(name, text)
    return res


def DumpCommand(database):
    """Creates a function to handle the DUMP command for a server.

    DUMP takes no arguments. It always returns a CSV containing all keys.

    Args:
      database: A KeyValueStore containing key/value pairs.
    Returns:
      A human readable string describing the result. If there is an error,
      then the string describes the error.
    """

    ##########################################
    # TODO: Think of error cases eg. no text, database, etc.
    ##########################################
    csv_format = ''
    for key in database.Keys():
        csv_format += key + ', '
    return csv_format.strip(', ')


def SendText(sock, text):
    """Sends the result over the socket along with a newline."""
    sock.send(text.encode() + b'\n')
              

def ProxyClientCommand(sock, server_addr, server_port, cache):
    """Receives a command from a client and forwards it to a server:port.

    A single command is read from `sock`. That command is passed to the specified
    `server`:`port`. The response from the server is then passed back through
    `sock`.

    Args:
      sock: A TCP socket that connects to the client.
      server_addr: A string with the name of the server to forward requests to.
      server_port: An int from 0 to 2^16 with the port the server is listening on.
      cache: A KeyValueStore object that maintains a temorary cache.
      max_age_in_sec: float. Cached values older than this are re-retrieved from
        the server.
    """
    command_line = library.ReadCommand(sock)
    cmd, name, text = library.ParseCommand(command_line)
    result = ''
    
    # Update the cache for PUT commands but also pass the traffic to the server.
    # GET commands can be cached.
    msg_to_server = (command_line, server_addr, server_port)
    if cmd == 'PUT':
        result = PutCommand(name, text, cache, msg_to_server)
    elif cmd == 'GET':
        result = GetCommand(name, cache, msg_to_server)
    elif cmd == 'DUMP':
        result = ForwardCommandToServer(msg_to_server)
    else:
        SendText(sock, 'Unknown command %s' % cmd)
    
    SendText(sock, result)
    

def main():
    # Listen on a specified port...
    server_sock = library.CreateServerSocket(LISTENING_PORT)
    cache = library.KeyValueStore()
    
    # Listens for connections.
    server_sock.listen(1)
    
    # Accept incoming commands indefinitely.
    while True:
        # Wait until a client connects and then get a socket that connects to the
        # client.
        client_sock, (address, port) = library.ConnectClientToServer(server_sock)
        print('Received connection from %s:%d' % (address, port))
        try:
            ProxyClientCommand(client_sock, SERVER_ADDRESS, SERVER_PORT, cache)

        finally:
            client_sock.close()

main()
