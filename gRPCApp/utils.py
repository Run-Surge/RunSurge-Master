import grpc

def get_peer_ip_and_port(peer: str):
    ip, port = peer[5:].split(":")
    return ip, int(port)



