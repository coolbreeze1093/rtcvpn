import socket
import struct
import random

def socks5_udp_associate(socks_host, socks_port):
    # 1. TCP 连接建立控制通道
    tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_sock.connect((socks_host, socks_port))
    
    # 2. 握手，无认证
    tcp_sock.send(b'\x05\x01\x00')
    resp = tcp_sock.recv(2)
    assert resp == b'\x05\x00', f"握手失败: {resp}"
    
    # 3. 发送 UDP ASSOCIATE 请求
    # VER=5, CMD=3(UDP ASSOCIATE), RSV=0, ATYP=1(IPv4), ADDR=0.0.0.0, PORT=0
    req = b'\x05\x03\x00\x01' + socket.inet_aton('0.0.0.0') + struct.pack('>H', 0)
    tcp_sock.send(req)
    resp = tcp_sock.recv(10)
    
    ver, rep, rsv, atyp = resp[0], resp[1], resp[2], resp[3]
    assert rep == 0, f"UDP ASSOCIATE 失败, rep={rep}"
    
    bnd_addr = socket.inet_ntoa(resp[4:8])
    bnd_port = struct.unpack('>H', resp[8:10])[0]
    print(f"服务器分配的 UDP relay 地址: {bnd_addr}:{bnd_port}")
    
    return tcp_sock, bnd_addr, bnd_port

def send_udp_via_socks5(bnd_addr, bnd_port, target_host, target_port, payload):
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    # 封装 SOCKS5 UDP 头部
    # RSV(2)=0x0000, FRAG(1)=0x00, ATYP(1)=0x01(IPv4), DST.ADDR(4), DST.PORT(2)
    header = b'\x00\x00\x00\x01' + socket.inet_aton(target_host) + struct.pack('>H', target_port)
    packet = header + payload
    
    udp_sock.sendto(packet, (bnd_addr, bnd_port))
    
    # 接收响应（如果目标是 echo 服务器之类）
    udp_sock.settimeout(3)
    try:
        data, addr = udp_sock.recvfrom(4096)
        print(f"收到响应 from {addr}: {data}")
        # 响应也会带 SOCKS5 UDP 头部，需要解析
        # 跳过头部（同样是4字节固定头+4字节IPv4+2字节端口=10字节）
        resp_payload = data[10:]
        print(f"实际负载: {resp_payload}")
    except socket.timeout:
        print("超时，没收到响应")
    
    udp_sock.close()


def build_dns_query(domain: str, qtype: int = 1) -> bytes:
    """
    构造一个标准 DNS 查询报文
    qtype: 1 = A 记录, 28 = AAAA 记录, 5 = CNAME, 15 = MX, 16 = TXT ...
    """
    # ---- Header (12 字节) ----
    transaction_id = random.randint(0, 0xFFFF)
    flags = 0x0100          # 标准查询，递归期望(RD=1)
    questions = 1
    answer_rrs = 0
    authority_rrs = 0
    additional_rrs = 0

    header = struct.pack(
        '>HHHHHH',
        transaction_id, flags, questions,
        answer_rrs, authority_rrs, additional_rrs
    )

    # ---- Question 部分 ----
    # 域名需要编码成 "长度前缀 label" 格式，例如:
    # www.baidu.com -> 03 www 05 baidu 03 com 00
    qname = b''
    for label in domain.strip('.').split('.'):
        label_bytes = label.encode('ascii')
        qname += struct.pack('B', len(label_bytes)) + label_bytes
    qname += b'\x00'   # 结尾 0 字节

    qclass = 1  # IN (Internet)
    question = qname + struct.pack('>HH', qtype, qclass)

    return header + question


# 示例


if __name__ == '__main__':
    # 修改成你的 SOCKS5 服务器地址
    tcp_sock, bnd_addr, bnd_port = socks5_udp_associate('127.0.0.1', 10800)

    dns_query = build_dns_query('www.baidu.com', qtype=1)
    print(dns_query.hex())
    
    # 测试发送到一个 UDP echo 服务器（比如你自己搭一个）
    send_udp_via_socks5(bnd_addr, bnd_port, '114.114.114.114', 53, dns_query)  # 示例DNS查询
    
    tcp_sock.close()