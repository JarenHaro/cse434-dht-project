import socket 
import sys

#dictionary to store valid peers
registered_peers = {}

#register function 
def register(peer_name: str, IPv4_addr:str, m_port:int, p_port:int) -> str:
    if peer_name in registered_peers or len(peer_name) > 15:
        return "FAILURE"

    if m_port == p_port:
        return "FAILURE"

    for peer in registered_peers.values():
        used_ports = (peer["m_port"], peer[p_port])

        if m_port in used_ports or p_port in used_ports:
            return "FAILURE"
    
    registered_peers[peer_name] = {
        "ipv4_addr": IPv4_addr,
        "m_port": m_port,
        "p_port": p_port,
        "state": "Free"
    }


#check to ensure program args include a port number
if (len(sys.argv) != 2):
    print("Manager.py must have only one argument: The Port Number")
    sys.exit(1)

#assign manager port number to user input
manager_port = int(sys.argv[1])

#create a socket for the manager 
manager_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

#bind the manager socket to accept messages from local and outside IP addresses on specified port number
manager_socket.bind(("0.0.0.0", manager_port))

print(f"Manager is waiting on port {manager_port} for messages...")

#infinite loop to read peer messages.
while True:
    data, sender_address = manager_socket.recvfrom(4096)
    message = data.decode("utf-8")

    print(f"Recieved from {sender_address}: {message}")

