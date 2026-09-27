import socket 
import sys
import random
import json

#dictionary to store valid peers
registered_peers = {}

#register function 
def register(peer_name: str, IPv4_addr:str, m_port:int, p_port:int) -> str:
    if peer_name in registered_peers or len(peer_name) > 15 or not peer_name.isalpha():
        return "FAILURE"

    if m_port == p_port:
        return "FAILURE"

    for peer in registered_peers.values():
        used_ports = (peer["m_port"], peer["p_port"])

        if m_port in used_ports or p_port in used_ports:
            return "FAILURE"
    
    registered_peers[peer_name] = {
        "ipv4_addr": IPv4_addr,
        "m_port": m_port,
        "p_port": p_port,
        "state": "Free"
    }
    return "SUCCESS"

def setup_dht(peer_name:str, n:int, year:int) -> tuple[str, list[tuple[str, str, int]]]:
    
    if peer_name not in registered_peers or n < 3 :
        return "FAILURE", []

    for peer in registered_peers.values():
        if peer["state"] != "Free":
            return "FAILURE", []

    free_peers = []
    for name, peer in registered_peers.items():
        if peer["state"] == "Free" and name != peer_name:
            free_peers.append(name)

    if len(free_peers) < n-1:
        return "FAILURE", []

    selected = random.sample(free_peers, n-1)

    leader = registered_peers[peer_name]
    leader["state"] = "Leader"
    dht_peers = [(peer_name, leader["ipv4_addr"], leader["p_port"])]

    for i in selected:
        peer = registered_peers[i]
        peer["state"] = "InDHT"
        dht_peers.append((i, peer["ipv4_addr"], peer["p_port"]))

    return "SUCCESS", dht_peers
    

def dht_complete(peer_name):
    pass

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
    message = json.loads(data.decode("utf-8"))


    print(f"Recieved from {sender_address}: {message}")

