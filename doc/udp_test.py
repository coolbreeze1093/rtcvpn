
#!/usr/bin/env python3

import socket
import struct
import sys
import time


# ============================================================
# 配置
# ============================================================

SOCKS5_HOST = "127.0.0.1"
SOCKS5_PORT = 10801

# 测试目标：Cloudflare DNS
TARGET_HOST = "1.1.1.1"
TARGET_PORT = 53

TIMEOUT = 5.0


# ============================================================
# 工具函数
# ============================================================

def hexdump(data: bytes) -> str:
    return " ".join(f"{b:02x}" for b in data)


def recv_exact(sock: socket.socket, n: int) -> bytes:
    data = b""

    while len(data) < n:
        chunk = sock.recv(n - len(data))

        if not chunk:
            raise ConnectionError("SOCKS5 server closed connection")

        data += chunk

    return data


def resolve_ipv4(host: str) -> str:
    return socket.gethostbyname(host)


# ============================================================
# SOCKS5 TCP handshake
# ============================================================

def socks5_handshake(sock: socket.socket):
    print("[1] SOCKS5 METHOD negotiation")

    # VER = 5
    # NMETHODS = 1
    # METHOD = 0 (NO AUTHENTICATION)
    request = bytes([
        0x05,
        0x01,
        0x00,
    ])

    print("    ->", hexdump(request))
    sock.sendall(request)

    response = recv_exact(sock, 2)

    print("    <-", hexdump(response))

    if response[0] != 0x05:
        raise RuntimeError(
            f"Invalid SOCKS version: {response[0]:02x}"
        )

    if response[1] != 0x00:
        raise RuntimeError(
            f"SOCKS5 authentication method rejected: "
            f"{response[1]:02x}"
        )

    print("    OK: NO AUTH")


# ============================================================
# UDP ASSOCIATE
# ============================================================

def socks5_udp_associate(sock: socket.socket):
    print("[2] SOCKS5 UDP ASSOCIATE")

    # RFC 1928:
    #
    # +----+------+------+----------+----------+
    # |VER | CMD  | RSV  | ATYP     | DST.ADDR |
    # +----+------+------+----------+----------+
    # | 1  |  1   |  2   |    1     | Variable |
    # +----+------+------+----------+----------+
    #
    # UDP ASSOCIATE:
    # CMD = 0x03
    #
    # 0.0.0.0:0 means:
    # "I don't know my UDP source address/port"

    request = struct.pack(
        "!BBBBIH",
        0x05,       # VER
        0x03,       # UDP ASSOCIATE
        0x00,       # RSV
        0x01,       # ATYP = IPv4
        0,          # DST.ADDR = 0.0.0.0
        0,          # DST.PORT = 0
    )

    print("    ->", hexdump(request))
    sock.sendall(request)

    header = recv_exact(sock, 4)

    ver = header[0]
    rep = header[1]
    rsv = header[2]
    atyp = header[3]

    if ver != 0x05:
        raise RuntimeError(f"Invalid response version: {ver:02x}")

    if rep != 0x00:
        error_map = {
            0x01: "general SOCKS server failure",
            0x02: "connection not allowed by ruleset",
            0x03: "network unreachable",
            0x04: "host unreachable",
            0x05: "connection refused",
            0x06: "TTL expired",
            0x07: "command not supported",
            0x08: "address type not supported",
        }

        raise RuntimeError(
            f"UDP ASSOCIATE failed: "
            f"0x{rep:02x} ({error_map.get(rep, 'unknown')})"
        )

    # Parse BND.ADDR
    if atyp == 0x01:
        addr = socket.inet_ntoa(
            recv_exact(sock, 4)
        )

    elif atyp == 0x03:
        length = recv_exact(sock, 1)[0]
        addr = recv_exact(sock, length).decode()

    elif atyp == 0x04:
        addr = socket.inet_ntop(
            socket.AF_INET6,
            recv_exact(sock, 16)
        )

    else:
        raise RuntimeError(
            f"Unsupported ATYP from server: {atyp:02x}"
        )

    port = struct.unpack(
        "!H",
        recv_exact(sock, 2)
    )[0]

    print(
        f"    <- VER=0x{ver:02x}, "
        f"REP=0x{rep:02x}, "
        f"ATYP=0x{atyp:02x}"
    )

    print(f"    UDP relay = {addr}:{port}")

    return addr, port


# ============================================================
# DNS packet
# ============================================================

def build_dns_query(domain: str = "example.com") -> tuple[int, bytes]:
    # 随机一点的 transaction ID
    transaction_id = 0x1234

    flags = 0x0100       # standard query
    qdcount = 1
    ancount = 0
    nscount = 0
    arcount = 0

    header = struct.pack(
        "!HHHHHH",
        transaction_id,
        flags,
        qdcount,
        ancount,
        nscount,
        arcount,
    )

    qname = b""

    for part in domain.split("."):
        encoded = part.encode("ascii")
        qname += bytes([len(encoded)])
        qname += encoded

    qname += b"\x00"

    # QTYPE = A
    # QCLASS = IN
    question = qname + struct.pack("!HH", 1, 1)

    return transaction_id, header + question


# ============================================================
# SOCKS5 UDP packet
# ============================================================

def build_socks5_udp_packet(
    target_host: str,
    target_port: int,
    payload: bytes,
) -> bytes:

    # RFC 1928 UDP request:
    #
    # +----+------+------+----------+----------+----------+
    # |RSV | FRAG | ATYP | DST.ADDR | DST.PORT |   DATA   |
    # +----+------+------+----------+----------+----------+
    #
    # RSV = 2 bytes
    # FRAG = 1 byte
    # ATYP = 1 byte

    ip = socket.inet_aton(target_host)

    header = struct.pack(
        "!HBB",
        0,          # RSV
        0,          # FRAG
        1,          # ATYP IPv4
    )

    header += ip

    header += struct.pack(
        "!H",
        target_port,
    )

    return header + payload


# ============================================================
# 解析 SOCKS5 UDP response
# ============================================================

def parse_socks5_udp_packet(data: bytes) -> bytes:
    if len(data) < 4:
        raise RuntimeError("UDP response too short")

    rsv = struct.unpack("!H", data[0:2])[0]
    frag = data[2]
    atyp = data[3]

    if rsv != 0:
        raise RuntimeError(
            f"Invalid RSV: {rsv}"
        )

    if frag != 0:
        raise RuntimeError(
            f"Fragmented UDP packet is not supported: FRAG={frag}"
        )

    offset = 4

    if atyp == 0x01:
        # IPv4
        if len(data) < offset + 4 + 2:
            raise RuntimeError("Invalid IPv4 UDP packet")

        addr = socket.inet_ntoa(
            data[offset:offset + 4]
        )
        offset += 4

    elif atyp == 0x03:
        # DOMAIN
        if len(data) < offset + 1:
            raise RuntimeError("Invalid DOMAIN UDP packet")

        length = data[offset]
        offset += 1

        if len(data) < offset + length + 2:
            raise RuntimeError("Invalid DOMAIN UDP packet")

        addr = data[offset:offset + length].decode()
        offset += length

    elif atyp == 0x04:
        # IPv6
        if len(data) < offset + 16 + 2:
            raise RuntimeError("Invalid IPv6 UDP packet")

        addr = socket.inet_ntop(
            socket.AF_INET6,
            data[offset:offset + 16]
        )
        offset += 16

    else:
        raise RuntimeError(
            f"Unknown ATYP: {atyp}"
        )

    port = struct.unpack(
        "!H",
        data[offset:offset + 2]
    )[0]

    offset += 2

    payload = data[offset:]

    print(
        f"    UDP response source = {addr}:{port}"
    )

    return payload


# ============================================================
# 主测试
# ============================================================

def main():
    print()
    print("======================================")
    print(" SOCKS5 UDP ASSOCIATE TEST")
    print("======================================")
    print()
    print(
        f"SOCKS5 : {SOCKS5_HOST}:{SOCKS5_PORT}"
    )
    print(
        f"TARGET : {TARGET_HOST}:{TARGET_PORT}"
    )
    print()

    # --------------------------------------------------------
    # TCP connection
    # --------------------------------------------------------

    print("[0] TCP connect to SOCKS5 server")

    tcp_sock = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM,
    )

    tcp_sock.settimeout(TIMEOUT)

    start = time.monotonic()

    tcp_sock.connect(
        (SOCKS5_HOST, SOCKS5_PORT)
    )

    print(
        f"    OK ({(time.monotonic() - start) * 1000:.2f} ms)"
    )

    try:
        # ----------------------------------------------------
        # SOCKS5 handshake
        # ----------------------------------------------------

        socks5_handshake(tcp_sock)

        # ----------------------------------------------------
        # UDP ASSOCIATE
        # ----------------------------------------------------

        relay_host, relay_port = socks5_udp_associate(
            tcp_sock
        )

        # ----------------------------------------------------
        # 如果服务端返回 0.0.0.0
        # 通常应该使用 SOCKS5 server 的 IP
        # ----------------------------------------------------

        if relay_host in ("0.0.0.0", "::"):
            relay_host = SOCKS5_HOST

            print(
                f"    relay address is wildcard, "
                f"use {relay_host}"
            )

        # ----------------------------------------------------
        # UDP socket
        # ----------------------------------------------------

        udp_sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM,
        )

        udp_sock.settimeout(TIMEOUT)

        # ----------------------------------------------------
        # DNS query
        # ----------------------------------------------------

        transaction_id, dns_query = build_dns_query(
            "example.com"
        )

        udp_packet = build_socks5_udp_packet(
            TARGET_HOST,
            TARGET_PORT,
            dns_query,
        )

        print()
        print("[3] Send UDP packet")

        print(
            f"    SOCKS5 UDP relay: "
            f"{relay_host}:{relay_port}"
        )

        print(
            f"    target: "
            f"{TARGET_HOST}:{TARGET_PORT}"
        )

        print(
            f"    payload size: "
            f"{len(dns_query)} bytes"
        )

        print(
            f"    packet size: "
            f"{len(udp_packet)} bytes"
        )

        print(
            "    packet:",
            hexdump(udp_packet)
        )

        start = time.monotonic()

        udp_sock.sendto(
            udp_packet,
            (relay_host, relay_port),
        )

        # ----------------------------------------------------
        # Receive response
        # ----------------------------------------------------

        print()
        print("[4] Waiting UDP response...")

        response, addr = udp_sock.recvfrom(65535)

        elapsed = (
            time.monotonic() - start
        ) * 1000

        print(
            f"    received from "
            f"{addr[0]}:{addr[1]}"
        )

        print(
            f"    size: {len(response)} bytes"
        )

        print(
            f"    latency: {elapsed:.2f} ms"
        )

        print(
            "    packet:",
            hexdump(response)
        )

        # ----------------------------------------------------
        # Parse SOCKS5 UDP response
        # ----------------------------------------------------

        print()
        print("[5] Parse SOCKS5 UDP response")

        dns_response = parse_socks5_udp_packet(
            response
        )

        print(
            f"    DNS payload size: "
            f"{len(dns_response)} bytes"
        )

        # ----------------------------------------------------
        # Check DNS transaction ID
        # ----------------------------------------------------

        if len(dns_response) >= 2:
            response_transaction_id = struct.unpack(
                "!H",
                dns_response[:2]
            )[0]

            if response_transaction_id == transaction_id:
                print(
                    "    DNS transaction ID: OK"
                )
            else:
                print(
                    "    WARNING: DNS transaction ID mismatch"
                )

        print()
        print("======================================")
        print(" SOCKS5 UDP TEST PASSED")
        print("======================================")

        udp_sock.close()

    finally:
        # UDP ASSOCIATE 的生命周期通常与这个
        # TCP control connection 绑定。
        tcp_sock.close()


if __name__ == "__main__":
    try:
        main()

    except socket.timeout:
        print()
        print("ERROR: timeout")
        print(
            "可能的问题："
        )
        print(
            "  - UDP ASSOCIATE 没成功"
        )
        print(
            "  - UDP relay 地址/端口错误"
        )
        print(
            "  - SOCKS5 UDP header 构造错误"
        )
        print(
            "  - UDP relay 没有转发数据"
        )
        print(
            "  - 目标服务器没有返回"
        )
        sys.exit(1)

    except Exception as e:
        print()
        print(
            f"ERROR: {type(e).__name__}: {e}"
        )
        sys.exit(1)

