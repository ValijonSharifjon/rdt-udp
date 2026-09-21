package com.rdtudp.sender;

import java.io.*;
import java.net.*;

import com.rdtudp.protocol.Corruptor;
import com.rdtudp.protocol.Packet;
import com.rdtudp.protocol.PacketCorruptedException;

public class StopAndWaitSender {
    private static final int TIMEOUT_MS  = 500;
    private static final int MAX_RETRIES = 10; 
    
    private final DatagramSocket socket;
    private final InetAddress serverAddress;
    private final int serverPort;
    private Corruptor corruptor;

    public StopAndWaitSender(String host, int port) throws IOException {
        this.socket = new DatagramSocket();

        this.socket.setSoTimeout(TIMEOUT_MS);
        this.serverAddress = InetAddress.getByName(host);
        this.serverPort    = port;
    }

    public void sendFile(String filePath) throws IOException {
        byte[] fileData = new FileInputStream(filePath).readAllBytes();
        int totalBytes = fileData.length;

        int totalChunks = (int) Math.ceil((double) totalBytes / Packet.MAX_PAYLOAD);

        System.out.printf("Sending file: %s (%d bytes, %d packets)%n",
            filePath, totalBytes, totalChunks);

        int totalRetries = 0;
        
        for (int seq = 0; seq < totalChunks; ++seq) {
            byte[] chunk = extractChunk(fileData, seq);

            Packet packet = new Packet();
            packet.sequenceNumber = seq;
            packet.payload = chunk;

            int retries = sendWithRetry(packet);
            totalRetries += retries;

            System.out.printf("  ACK SEQ=%d (%d bytes)%s%n",
                seq, chunk.length,
                retries > 0 ? " [retries: " + retries + "]" : "");
        }

        sendFin(totalChunks);

        System.out.printf("Done. Total retries: %d%n", totalRetries);
    }

    public void setCorruptor(Corruptor corruptor) {
        this.corruptor = corruptor;
    }

    private int sendWithRetry(Packet packet) throws IOException {
        byte[] raw = packet.serialize();

        for (int attempt = 0; attempt <= MAX_RETRIES; ++attempt) {
            sendDatagram(raw);

            try {
                Packet ack = receiveAck();

                if (ack.hasFlag(Packet.FLAG_ACK) && ack.ackNumber == packet.sequenceNumber + 1) {
                    return attempt;
                }

                System.out.printf("    Unexpected ACK=%d, waiting for %d%n",
                    ack.ackNumber, packet.sequenceNumber + 1);

                
            } catch(SocketTimeoutException e) {
                System.out.printf("    Timeout for SEQ=%d, attempt %d/%d%n",
                    packet.sequenceNumber, attempt + 1, MAX_RETRIES);
                    
            } catch (PacketCorruptedException e) {
                System.out.printf("    Corrupted ACK: %s%n", e.getMessage());
            }
        }

        throw new IOException(
            "Failed to deliver packet SEQ=" + packet.sequenceNumber 
            + " after " + MAX_RETRIES + " attempts"
        );
    }

    private void sendFin(int nextSeq) throws IOException {
        Packet fin = new Packet();
        fin.sequenceNumber = nextSeq;
        fin.setFlag(Packet.FLAG_FIN);

        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            sendDatagram(fin.serialize());
            try {
                Packet response = receiveAck();
                if (response.hasFlag(Packet.FLAG_FIN) && response.hasFlag(Packet.FLAG_ACK)) {
                    System.out.println("FIN+ACK received — connection closed.");
                    return;
                }
            } catch (SocketTimeoutException e) {
                System.out.printf("  Timeout on FIN, attempt %d/%d%n", attempt + 1, MAX_RETRIES);
            } catch (PacketCorruptedException e) {
                System.out.println("  Corrupted response to FIN");
            }
        }
        throw new IOException("Server did not acknowledge FIN");
    }

    private void sendDatagram(byte[] raw) throws IOException {
        byte[] toSend = raw;

        if (corruptor != null) {
            toSend = corruptor.apply(raw);
        }

        if (toSend == null) {
            System.out.println("    ⚡ DROP: packet dropped");
            return;
        }

        DatagramPacket datagram = new DatagramPacket(
            toSend,
            toSend.length,
            serverAddress,
            serverPort
        );

        socket.send(datagram);
    }

    private Packet receiveAck() throws IOException, PacketCorruptedException {
        byte[] buf = new byte[Packet.MAX_PACKET];
        DatagramPacket datagram = new DatagramPacket(buf, buf.length);

        socket.receive(datagram); 

        byte[] received = new byte[datagram.getLength()];
        System.arraycopy(buf, 0, received, 0, datagram.getLength());

        return Packet.deserialize(received);
    }

    private byte[] extractChunk(byte[] fileData, int chunkIndex) {
        int from = Packet.MAX_PAYLOAD * chunkIndex;
        int to = Math.min(from + Packet.MAX_PAYLOAD, fileData.length);

        return java.util.Arrays.copyOfRange(fileData, from, to);
    }

    public void close() {
        socket.close();
    }
}
