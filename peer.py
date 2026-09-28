import socket
import sys
import json
import threading


if len(sys.argv) != 3:
    print("peer.py must have two arguments: <manager-ip> <manager-port>\n")
    sys.exit(1)

manager_address = (sys.argv[1], int(sys.argv[2]))

ring_is_ready = threading.Event()

ring_state = {
    "id": None,
    "size": None,
    "peers": [],
    "right_neighbor": None
}

def ring_setup(peer_id, peers):
    ring_size = len(peers)
    right_id = (peer_id + 1) % ring_size
    ring_state["id"] = peer_id
    ring_state["size"] = ring_size
    ring_state["peers"] = peers
    ring_state["right_neighbor"] = peers[right_id]

    neighbor = peers[right_id]

    print(f"\nRing configured: ID= {peer_id}, size={ring_size}," f"right neighbor={neighbor[0]} at  {neighbor[1]}:{neighbor[2]}")

def receive_peer_messages(peer_socket: socket.socket):
    while True:
        try:
            data, sender_address = peer_socket.recvfrom(4096)
        except OSError:
            return

        try:
            message = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            print(f"\n Invalid peer message from {sender_address}")
            continue
        if message.get("command") == "set-id":
            ring_setup(message["id"], message["peers"])
        elif message.get("command") == "ring-is-ready":
            if ring_state["id"] is None:
                continue

            if ring_state["id"] == 0:
                print("\nReadiness message returned. All peers are configured.")
                ring_is_ready.set()
            else:
                neighbor = ring_state["right_neighbor"]

                peer_socket.sendto(json.dumps(message).encode("utf-8"), (neighbor[1], neighbor[2]))

                print(f"\nForwarded ring-is-ready to {neighbor[0]}")

        print(f"\nPeer message from {sender_address}: {message}")

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

                receiver_thread = threading.Thread(target=receive_peer_messages, args=(peer_socket,), daemon=True)
                receiver_thread.start()

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
            peer_name = parts[1]
            if registered_name != peer_name:
                print("Use this peer's registered name.")
                continue

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
        # successful setup response code 
        try:
            data, sender_address = manager_socket.recvfrom(4096)
            response = json.loads(data.decode("utf-8"))
            if command == "register" and response["status"] == "SUCCESS":
                registered_name = peer_name
            if command == "setup-dht" and response["status"] == "SUCCESS":
                dht_year = year
                dht_peers = response["peers"]

                ring_is_ready.clear()
                print("DHT peers:", dht_peers)
                ring_setup(0, dht_peers)

                for peer_id in range(1, len(dht_peers)):
                    name, ip_address, p_port = dht_peers[peer_id]

                    set_id_message = {
                        "command": "set-id",
                        "id": peer_id,
                        "n": len(dht_peers),
                        "peers": dht_peers
                    }
                    peer_socket.sendto(
                        json.dumps(set_id_message).encode("utf-8"), (ip_address, p_port)
                    )

                    print(f"Sent set-id to {name}: ID={peer_id}")
                neighbor = ring_state["right_neighbor"]
                ready_message = {"command": "ring-is-ready"}

                for check in range(3):
                    peer_socket.sendto(
                        json.dumps(ready_message).encode("utf-8"), (neighbor[1], neighbor[2])
                    )

                    print(f"Sent ring-is-ready check to {neighbor[0]}")

                    if ring_is_ready.wait(timeout=3):
                        break
                else:
                    print("Ring check timed out. Try restarting and testing again.")
                    continue
                print("All peers are ready. Safe to start distributing records.")

            print("Manager response:", response["status"])
        except socket.timeout:
            print("No response from the manager within 5 seconds.")