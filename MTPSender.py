import threading
import unreliable_channel
import socket
import zlib
import struct
import time
import sys

class PacketSender:

    def __init__(self,input_file, ip_address, window_size, port_number, log_file, seq_number,):
        # define and init
        self.ip_address = ip_address
        self.window_size = int(window_size)
        self.port_number = int(port_number)
        self.input_file = input_file
        self.log_file = log_file
        self.seq_number = seq_number
        self.lock = threading.Lock()

         # opening log file to start logging
        self.filelogging = open(log_file,'w')

        #dict for header and data
        self.window_pkt = {}
        self.received_pkts = []
        self.sent = 0
        self.left_window = 0
        self.right_window = 0
        self.tot_packets = 0
        self.receivedflag = 0 # 1 || 3

    def initialize_windowlist(self, tot_packets):
        for pkt in range(tot_packets):
            self.window_pkt[pkt] = 1

    def print_windowlist(self, tot_packets):
        self.filelogging.write("Window state:[")
        for pkt in range(self.left_window_count, self.right_window):
            self.filelogging.write("%d(%d),", pkt, self.window_pkt[pkt])
        self.filelogging.write("]\n")

    # breaks up the file into chunks
    def create_packet(self):
        data_size = 1456
        packet = {}

        with open(self.input_file, 'rb') as input:
            while True:
                data = input.read(data_size)
                if not data:
                    break

                self.seq_number += 1
                length = len(data)
                checksum = zlib.crc32(data)

                header = struct.pack('!IIII', 1, self.seq_number, length+16, checksum)
                packet[self.seq_number] = header + data
    
        return packet
    
    
    def ack_received_packet(self, ty, seq, length, checksum_in_packet):
        checksum_calculated = zlib.crc32(struct.pack('!III', ty, seq, length))
        if (checksum_in_packet == checksum_calculated):

            right_window = min(right_window + 1, len(self.packet)-1)
            if right_window - self.left_window >= self.window_size: # maintain window size
                left_window += 1
            self.received_pkts = None # reset received packets list

            self.lock.acquire()
            self.filelogging.write("Updating window;(show seqNum of %d packets in the window with one bit status (0: sent but not acked, 1:not sent)\n", self.tot_packets)
            self.print_windowlist(self.tot_packets)
            self.filelogging.write(f"Packet received; type={ty}; seqNum={seq}; length={length}; checksum_in_packet={checksum_in_packet}""\n")
            self.lock.release()

        else: # ignore corrupt files
            self.lock.acquire()
            self.filelogging.write(f"Packet received; type={ty}; seqNum={seq}; length={length}; checksum_in_packet={checksum_in_packet}; checksum_calculated={checksum_calculated}; status=CORRUPT; \n")
            self.lock.release()
            
        self.receivedflag = 1
        return left_window, right_window, 1 # flag = 1, no resending
        
    
    def extract_packet(self):
        if self.received_pkts: # if false, then timeout so resend packet
            for ack in self.received_pkts:
                ty, seq, length, checksum_in_packet = struct.unpack('!IIII', ack)
                if self.left_window == seq:
                   self.ack_received_packet(ty, seq, length, checksum_in_packet)
                else: # check for triple acks
                    if seq in self.seen_acks:
                        self.seen_acks[seq] += 1
                        if self.seq_acks[seq] >= 3:
                            self.lock.acquire()
                            self.filelogging.write("Triple dup acks received for packet seqNum=%d", seq)
                            self.lock.release()
                    else:
                        self.seen_acks[seq] = 1
        else:
            self.lock.acquire()
            self.filelogging.write("Timeout for packet seqNum=%d",self.left_window)
            self.lock.release()

        self.received_pkts = None

    def receive_thread(self, clientsocket):
        while True:
            packet_from_server, _ = unreliable_channel.recv_packet(clientsocket)
            self.received_pkts.append(packet_from_server)
            self.extract_packet()

    def main(self):

        packet = processor.create_packet()

        # open client socket and bind
        clientsocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # clientsocket.bind((self.ip_address, self.port_number))
        
        # start receive thread
        recv_thread = threading.Thread(target=self.receive_thread,args=(clientsocket,))
        recv_thread.start()
        self.tot_packets = len(packet)
        self.right = min(self.tot_packets, self.left_window + self.window_size)-1
        self.initialize_windowlist(self.tot_packets)
        curr_pkt = self.left_window

        while self.sent < self.tot_packets:

            while curr_pkt <= self.right_window:
                unreliable_channel.send_packet(clientsocket, packet[curr_pkt], (self.ip_address, self.port_number)) # add receiver addy
                self.window_pkt[curr_pkt] = 0
                ty, seq, length, checksum = struct.unpack('!IIII',  packet[curr_pkt][:16])
                self.lock.acquire()
                self.filelogging.write(f"Packet sent; type={ty}; seqNum={seq}; length={length}; checksum={checksum}""\n")
                self.lock.release()

                start_time = time.time()
                while True:
                    recv_thread.join(timeout=0.5)
                    if time.time() - start_time >= 0.5:
                        break
                        
                if self.receivedflag != 1 or self.receivedflag != 2: # if timeout or dupAcks
                    curr_pkt = self.left_window
                else: # received or corrupted
                    curr_pkt += 1

            self.sent += 1
        
        self.filelogging.close()
        clientsocket.close()

if __name__ == "__main__":
    # read data from command line args
    if len(sys.argv) != 6:
        print("Wrong number of arguments")
        sys.exit(1)

    ip_address = sys.argv[1]
    port_number = sys.argv[2]
    window_size = sys.argv[3]
    input_file = sys.argv[4]
    log_file = sys.argv[5]
    seq_num = -1

    # read input file and split it into packets
    processor = PacketSender(input_file, ip_address, window_size, port_number, log_file, seq_num)
    processor.main()