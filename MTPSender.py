import threading
import unreliable_channel
import socket
import zlib
import struct
import time
import sys

class PacketSender:

    lock = threading.Lock()

    def __init__(self,input_file, log_file, seq_number, type):
        # define and init
        self.input_file = input_file
        self.log_file = log_file
        self.seq_number = seq_number

        self.lock = threading.Lock()
        #dict for header and data
        self.windowPkt = {}
        self.received_pkts = []

    # breaks up the file into chunks
    def create_packet(self):
        data_size = 1462
        packet = {}

        with open(self.input_file, 'rb') as input:
            while True:
                data = input.read(data_size)    
                if not data: 
                    break

                self.seq_number += 1
                length = len(data)
                encoded_data = data.encode('utf-8')
                checksum = zlib.crc32(encoded_data)

                headerData = struct.pack('!IIII', 1, self.seq_number, length, checksum, data)
                packet[self.seq_number] = headerData
    
        return packet

    def extract_packet(self):
        # extract the packet data after receiving
        for data in self.received_pkts:
            ty, seq, length, checksum = struct.unpack('!IIII', data)
            mychecksum = zlib.crc32(seq,length,ty)
            if (checksum == mychecksum):
                self.packet[seq]
            else: # corrupt file
        return

    def receive_thread(self, UDPClientSocket):
        while True:
            packet_from_server, server_address = unreliable_channel.recv_packet(UDPClientSocket)
            self.received_pkts.append(packet_from_server)

    def main(self):

        packet = processor.create_packet()
        
        # opening log file to start logging
        fLog = open(log_file,'a')
        fLog.write("Starting\n")

        # open client socket and bind
        UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        UDPClientSocket.bind((ip_address, port_number))
        
        # start receive thread
        recv_thread = threading.Thread(target=self.receive_thread,args=(UDPClientSocket,))
        
        tot_packets = len(packet) 
        left_window = 0 
        right_window = min(tot_packets, left_window + window_size)-1
        curr_pkt = left_window;
        while len(self.windowPkt) != tot_packets:
            start_time = time.time()
            recv_thread.start()
            while time.time() - start_time < 500/1000:
                while curr_pkt <= right_window:
                    unreliable_channel.send_packet(UDPClientSocket, packet[curr_pkt], ip_address) # add receiver addy
                    self.lock.aquire()
                    # log info to fLog
                    self.lock.release()
                    curr_pkt += 1
            
            #process packet after time out - double checking time out
            if time.time() - start_time >= 500/1000:
                left, right, curr = self.extract_packet()
                    
            #update window,left and right
            left_window = left
            right_window = right
            curr_pkt = curr
            tot_packets -= 1
            
if __name__ == "__main__":
    # read data from command line args
    if len(sys.argv) != 6:
        print("Wrong number of arguments")
        sys.exit(1)

    ip_address = sys.argv[1];
    port_number = sys.argv[2];
    window_size = sys.argv[3];
    input_file = sys.argv[4];
    log_file = sys.argv[5];
    seq_num = -1;

    # read input file and split it into packets
    processor = PacketSender(input_file, log_file, seq_num) 
    processor.main()