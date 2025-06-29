import urllib.parse
import ipaddress

def parse_ipv6_address(ipv6_address_string):
    """
    Parses the IPv6 address string into a (IPAddress object, port) tuple.
    """
    stripped_prefix = ipv6_address_string[len("ipv6:"):]
    decoded_string = urllib.parse.unquote(stripped_prefix)

    ip_str, port_str = decoded_string.split(']:', 1)
    ip_str = ip_str + ']'
    print(ip_str, port_str)
    return ip_str, int(port_str)

def parse_ipv4_address(ipv4_address_string):
    """
    Parses the IPv4 address string into a (IPAddress object, port) tuple.
    """
    stripped_prefix = ipv4_address_string[len("ipv4:"):]
    decoded_string = urllib.parse.unquote(stripped_prefix)

    ip_str, port_str = decoded_string.split(':', 1)
    return ip_str, int(port_str)

def parse_grpc_peer_address(grpc_address_string):
    """
    Parses the gRPC peer address string into a (IPAddress object, port) tuple.
    """
    if grpc_address_string.startswith("ipv6:"):
        return parse_ipv6_address(grpc_address_string)
    elif grpc_address_string.startswith("ipv4:"):
        return parse_ipv4_address(grpc_address_string)
    else:
        raise ValueError("Unsupported gRPC address format")