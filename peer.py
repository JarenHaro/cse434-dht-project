import socket
import sys
import json

if len(sys.argv) != 3:
    print("peer.py must have two arguments: <manager-ip> <manager-port>\n")
    sys.exit(1)

manager_address = (sys.argv[1], int(sys.argv[2]))



with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as manager_socket, \
     socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as peer_socket:
    
    registered_name = None
    already_bound_sockets = False
    bound_m_port = None
    bound_p_port = None
    while True:
        parts = input("Enter command: ").split()
        if not parts:
            continue

        command = parts[0]
        if command == "register":
            if registered_name is not None:
                print(f"This process is already registered as {registered_name}.")
                continue

            if len(parts) != 5:
                print("register command must have 4 arguments: <peer_name> <IPv4_addr> <m_port> <p_port>")
                continue
            
            peer_name = parts[1]
            ipv4_addr = parts[2]
            try:
                m_port = int(parts[3])
                p_port = int(parts[4])
            except ValueError:
                print("Ports must be integers.")
                continue

            if m_port == p_port:
                print("manager and peer ports must be different.")
                continue

            if not (11500 <= m_port <= 11999 and 11500 <= p_port <= 11999):
                print("Both ports must be within 11500 - 11999.")
                continue

            if already_bound_sockets:
                if m_port != bound_m_port or p_port != bound_p_port:
                    print("Retry with the same port numbers or restart the peer to change them.")
                    continue
            else:
                try:
                    manager_socket.bind(("0.0.0.0", m_port))
                    peer_socket.bind(("0.0.0.0", p_port))
                except OSError as error:
                    print(f"Could not bind sockets: {error}")
                    print("Restart this peer with available ports.")
                    break

                already_bound_sockets = True
                bound_m_port = m_port
                bound_p_port = p_port

            message = {
            "command": "register",
            "peer_name": peer_name,
            "ipv4_addr": ipv4_addr,
            "m_port": m_port,
            "p_port": p_port
            }
        elif command == "setup-dht":
            if len(parts) != 4:
                print("setup-dht command must have 3 arguments: <peer_name> <n> <year>")
                continue
            peer_name = parts[1]
            if registered_name is None:
                print("Register successfully first.")
                continue
            if registered_name != peer_name:
                print("Use this peer's registered name.")
                continue
            try:
                n = int(parts[2])
                year = int(parts[3])
            except ValueError:
                print("n and year must be integers.")
                continue

            message = {
            "command": "setup-dht",
            "peer_name": peer_name,
            "n": n,
            "year": year
            }
        elif command == "dht-complete":
            if len(parts) != 2:
                print("dht-complete command must specify <peer_name>")
                continue
            if registered_name is None:
                print("Register successfully first.")
                continue
            if registered_name != peer_name:
                print("Use this peer's registered name.")
                continue
            peer_name = parts[1]

            message = {
                "command": "dht-complete",
                "peer_name": peer_name
            }
        else:
            print("unknown command.")
            continue

        manager_socket.settimeout(10)

        manager_socket.sendto(
            json.dumps(message).encode("utf-8"),
            manager_address
        )

        try:
            data, sender_address = manager_socket.recvfrom(4096)
            response = json.loads(data.decode("utf-8"))
            if command == "register" and response["status"] == "SUCCESS":
                registered_name = peer_name
            if command == "setup-dht" and response["status"] == "SUCCESS":
                dht_peers = response["peers"]
                print("DHT peers:", dht_peers)
            print("Manager response:", response["status"])
        except socket.timeout:
            print("No response from the manager within 5 seconds.")