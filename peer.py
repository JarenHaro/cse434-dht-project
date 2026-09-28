import socket
import sys
import json
import threading
import csv 
from pathlib import Path


if len(sys.argv) != 3:
    print("peer.py must have two arguments: <manager-ip> <manager-port>\n")
    sys.exit(1)

manager_address = (sys.argv[1], int(sys.argv[2]))

ring_is_ready = threading.Event()

counts_received = threading.Event()
actual_counts = []

ring_state = {
    "id": None,
    "size": None,
    "peers": [],
    "right_neighbor": None
}

local_hash_table = {}

#loads the storm records 
def load_storm_records(year: int) -> list:
    filename = Path(__file__).resolve().parent / f"details-{year}.csv"

    with filename.open("r", newline="", encoding="utf-8-sig") as file:
        reader = csv.reader(file)
        next(reader, None)

        records = []

        for row in reader:
            if not row:
                continue

            if len(row) != 14:
                raise ValueError(
                    f"Expected 14 columns, got {len(row)} "
                    f"near line {reader.line_num}."
                )

            event_id = int(row[0])
            records.append([event_id, *row[1:]])

    return records

#helper function for hashing
def first_prime_above(number: int) -> int:
    candidate = max(2, number + 1)

    while True:
        is_prime = True
        divisor = 2

        while divisor * divisor <= candidate:
            if candidate % divisor == 0:
                is_prime = False
                break

            divisor += 1

        if is_prime:
            return candidate

        candidate += 1

def ring_setup(peer_id, peers):
    ring_size = len(peers)
    right_id = (peer_id + 1) % ring_size
    ring_state["id"] = peer_id
    ring_state["size"] = ring_size
    ring_state["peers"] = peers
    ring_state["right_neighbor"] = peers[right_id]

    neighbor = peers[right_id]

    print(f"\nRing configured: ID= {peer_id}, size={ring_size}," f"right neighbor={neighbor[0]} at  {neighbor[1]}:{neighbor[2]}")

#implements the storage of records onto respective hash tables
def store_record(pos, record):
    if pos not in local_hash_table:
        local_hash_table[pos] = []

    local_hash_table[pos].append(record)
    print(f"Stored event {record[0]} at position {pos}")


def send_to_right(peer_socket, message):
    name, ip_address, port = ring_state["right_neighbor"]

    peer_socket.sendto(
        json.dumps(message).encode("utf-8"),
        (ip_address, port)
    )

    print(f"Sent {message['command']} to right neighbor {name}")

    

def count_local_records():
    return sum(len(bucket) for bucket in local_hash_table.values())

def receive_peer_messages(peer_socket: socket.socket):
    while True:
        try:
            data, sender_address = peer_socket.recvfrom(4096)
        except OSError:
            return

        try:
            message = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            print(f"\nInvalid peer message from {sender_address}")
            continue

        command = message.get("command")
        print(f"\nReceived {command} from {sender_address}")

        if command == "set-id":
            ring_setup(message["id"], message["peers"])

        elif command == "ring-is-ready":
            if ring_state["id"] is None:
                continue

            if ring_state["id"] == 0:
                print("Readiness message returned. All peers are configured.")
                ring_is_ready.set()
            else:
                send_to_right(peer_socket, message)

        elif command == "store":
            pos = message["pos"]
            record = message["record"]
            destination_id = pos % ring_state["size"]

            if destination_id == ring_state["id"]:
                store_record(pos, record)
            elif ring_state["id"] == 0:
                print(f"ERROR: Event {record[0]} returned without being stored.")
            else:
                send_to_right(peer_socket, message)
        elif command == "collect-counts":
            if ring_state["id"] == 0:
                actual_counts.clear()
                actual_counts.extend(message["counts"])
                counts_received.set()
            else:
                count = count_local_records()
                message["counts"].append(count)

                print(f"Peer ID {ring_state['id']} has {count} records.")
                send_to_right(peer_socket, message)

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

                try:
                    records = load_storm_records(dht_year)
                except (OSError, ValueError) as error:
                    print(f"Could not load dataset: {error}")
                    print("Fix the file issue and restart the test.")
                    continue

                record_count = len(records)
                table_size = first_prime_above(2 * record_count)

                print(f"Loaded {record_count} storm records.")
                print(f"Hash table size: {table_size}")

                expected_counts = [0] * ring_state["size"]

                for record in records:
                    event_id = record[0]
                    pos = event_id % table_size
                    destination_id = pos % ring_state["size"]

                    expected_counts[destination_id] += 1

                for peer_id, count in enumerate(expected_counts):
                    print(f"Peer ID {peer_id} should receive {count} records.")

                for record in records:
                    event_id = record[0]
                    pos = event_id % table_size
                    destination_id = pos % ring_state["size"]

                    if destination_id == ring_state["id"]:
                        store_record(pos, record)
                    else:
                        neighbor = ring_state["right_neighbor"]

                        store_message = {
                            "command": "store",
                            "pos": pos,
                            "record": record
                        }

                        peer_socket.sendto(
                            json.dumps(store_message).encode("utf-8"),
                            (neighbor[1], neighbor[2])
                        )

                        print(f"Sent event {event_id} to right neighbor {neighbor[0]}")

                print("Finished sending records; storage counts still need verification.")
                counts_received.clear()
                actual_counts.clear()

                count_message = {
                    "command": "collect-counts",
                    "counts": [count_local_records()]
                }

                send_to_right(peer_socket, count_message)

                if not counts_received.wait(timeout=10):
                    print("Count collection timed out. DHT not marked complete.")
                    continue

                for peer_id, count in enumerate(actual_counts):
                    name = dht_peers[peer_id][0]
                    print(f"Peer {name}, ID {peer_id}: {count} records stored.")

                print(f"Total records stored: {sum(actual_counts)}")

                if actual_counts != expected_counts:
                    print(f"Expected counts: {expected_counts}")
                    print(f"Actual counts:   {actual_counts}")
                    print("Counts do not match. DHT not marked complete.")
                    continue

                completion_message = {
                    "command": "dht-complete",
                    "peer_name": registered_name
                }

                manager_socket.sendto(
                    json.dumps(completion_message).encode("utf-8"),
                    manager_address
                )

                print("Sent dht-complete to manager.")

                try:
                    data, sender_address = manager_socket.recvfrom(4096)
                except socket.timeout:
                    print("No completion response from manager within 10 seconds.")
                    continue

                completion_response = json.loads(data.decode("utf-8"))

                if completion_response["status"] == "SUCCESS":
                    print("DHT setup completed successfully.")
                else:
                    print("Manager rejected dht-complete.")
            print("Manager response:", response["status"])
        except socket.timeout:
            print("No response from the manager within 5 seconds.")