registered_peers = {}

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

assert register("Jaren", "127.0.0.1", 11501, 11502) == "SUCCESS"

assert register("Jaren", "127.0.0.1", 11503, 11504) == "FAILURE"

assert register("Peer1", "127.0.0.1", 11503, 11504) == "FAILURE"
assert register("A" * 16, "127.0.0.1", 11503, 11504) == "FAILURE"
assert register("", "127.0.0.1", 11503, 11504) == "FAILURE"

assert register("Alex", "127.0.0.1", 11503, 11503) == "FAILURE"

assert register("Alex", "127.0.0.1", 11501, 11504) == "FAILURE"
assert register("Alex", "127.0.0.1", 11502, 11504) == "FAILURE"

assert register("Alex", "127.0.0.1", 11503, 11501) == "FAILURE"
assert register("Alex", "127.0.0.1", 11503, 11502) == "FAILURE"

assert register("A" * 15, "127.0.0.1", 11503, 11504) == "SUCCESS"

assert len(registered_peers) == 2
assert registered_peers["Jaren"] == {
    "ipv4_addr": "127.0.0.1",
    "m_port": 11501,
    "p_port": 11502,
    "state": "Free"
}

print("All registration tests passed!")