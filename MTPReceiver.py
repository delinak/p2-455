

import unreliable_channel
import sys
import struct
import threading
import zlib
from socket import *

thread_lock = threading.Lock()
file_lock = threading.Lock()
second_packet_received = threading.Event()
first_packet_received = threading.Event()

def create_packet(seq):
    sending_type = 1
    seqNum = seq
    length = 16
    checksum_in_packet = calculate_checksum(struct.pack('!III', sending_type, seqNum, length))
    header = struct.pack('!IIII', sending_type, seqNum, length, checksum_in_packet)
    unpacked = struct.unpack('!IIII', header)
    
    return header, unpacked
    
def extract_packet_info(data):
    header = struct.unpack('!IIII', data[:16])
    return header, data[16:]

def send_acknowledgment(socket, addr, data, second, log, unpacked, unexpected):
    global thread_lock, second_packet_received, first_packet_received, file_lock
    if unexpected:
        thread_lock.acquire()
        for i in range(2):
            unreliable_channel.send_packet(socket, data, addr)
            log.write("Packet sent; type=ACK;seqNum=%d;length=16;checksum_in_packet=%#x\n" % (unpacked[1], unpacked[3]))
        thread_lock.release()
        return
        
    second_packet_received.wait(0.5)
    
    thread_lock.acquire()
    if second_packet_received.is_set() and not second:
        thread_lock.release()
        return
    first_packet_received.clear()
    second_packet_received.clear()
    unreliable_channel.send_packet(socket, data, addr)
    file_lock.acquire()
    log.write("Packet sent; type=ACK;seqNum=%d;length=16;checksum_in_packet=%#x\n" % (unpacked[1], calculate_checksum(data)))
    file_lock.release()
    thread_lock.release()
    

def calculate_checksum(data):
    return zlib.crc32(data)
    
def main():
    global thread_lock, second_packet_received, first_packet_received
    
    packet_status = ["CORRUPT", "NOT_CORRUPT", "OUT_OF_ORDER_PACKET"]
    
    log = open(sys.argv[3], 'w')
    output = open(sys.argv[2], 'w')
    serverPort = sys.argv[1]
    
    receiver_socket = socket(AF_INET, SOCK_DGRAM)
    receiver_socket.bind(('', int(serverPort)))
    expected = 0
    end = False
    
    while True:
        received_packet, senderAddress = unreliable_channel.recv_packet(receiver_socket)
        header, data = extract_packet_info(received_packet)
        checksum_in_packet = calculate_checksum(data)
        status = 0 if header[3] != checksum_in_packet else 1
        status = 2 if expected != header[1] else status
        unexpected = status != 1
        end = header[2] < 1472
        file_lock.acquire()
        log.write("Packet received; type=DATA;seqNum=%d;length=%d;checksum_in_packet=%d;checksum_calculated=%d;status=%s\n" % (header[1], header[2], header[3], checksum_in_packet, packet_status[status]))
        file_lock.release()
        packet_to_send, unpacked = create_packet(expected - 1 if unexpected else expected)
        
        if not first_packet_received.is_set():
            first_packet_received.set()
            second = False
        else:
            second_packet_received.set()
            first_packet_received.clear()
            second = True
			
        ack_thread = threading.Thread(target=send_acknowledgment, args=(receiver_socket, packet_to_send, senderAddress, second, log, unpacked, unexpected))
        ack_thread.start()
        
        if not unexpected:
            expected += 1
            output.write(data.decode())
        if end:
            ack_thread.join()
            break
    log.close()
    output.close()
        
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python3 MTPReceiver.py <receiver-port> <output-file> <receiver-log-file>")
        sys.exit(1)
    main()