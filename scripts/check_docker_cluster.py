
import socket
import time

HOST = "127.0.0.1"
PORTS = [6380, 6381, 6382]
KEY = "fa2_m22_replication_check"
VALUE = "docker_cluster_ok"


def encode_command(*args):
    payload = f"*{len(args)}\r\n"
    for arg in args:
        encoded = arg.encode()
        payload += f"${len(encoded)}\r\n{arg}\r\n"
    return payload.encode()


def command(port, *args):
    with socket.create_connection((HOST, port), timeout=3) as sock:
        sock.settimeout(3)
        sock.sendall(encode_command(*args))
        return sock.recv(16384).decode(errors="replace").strip()


print("SET on node-a:")
print(command(6380, "SET", KEY, VALUE))

print("\nChecking replication on all nodes:")
for port in PORTS:
    value = command(port, "GET", KEY)
    print(f"Port {port}: {value}")

print("\nSTALENESS report from node-a:")
print(command(6380, "STALENESS"))
