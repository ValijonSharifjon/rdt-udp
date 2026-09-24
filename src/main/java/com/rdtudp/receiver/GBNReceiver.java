package com.rdtudp.receiver;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.TreeMap;

import com.rdtudp.protocol.Packet;
import com.rdtudp.protocol.PacketCorruptedException;

public class GBNReceiver {
    private final int port;

    public GBNReceiver(int port) {
        this.port = port;
    }

    public void receive(String outputPath) throws IOException {
        DatagramSocket socket = new DatagramSocket(port);

        System.out.println("GBN Receiver listening on port " + port + "...");

        TreeMap<Integer, byte[]> chunks = new TreeMap<>();
        int expectedSeq = 0;
        int lastAck = -1;

        byte[] buf = new byte[Packet.MAX_PACKET];

        while (true) {
            DatagramPacket datagram = new DatagramPacket(buf, buf.length);

            try {
                socket.receive(datagram);
            } catch (IOException e) {
                System.err.println("Receive error: " + e.getMessage());
                continue;
            }

            byte[] received = new byte[datagram.getLength()];
            System.arraycopy(buf, 0, received, 0, datagram.getLength());

            Packet packet;
            try {
                packet = Packet.deserialize(received);
            } catch (PacketCorruptedException e) {
                System.out.println("  ✗ Corrupt — retransmitting ACK=" + lastAck);
                if (lastAck >= 0) {
                    sendAck(socket, datagram.getAddress(), datagram.getPort(), lastAck);
                }
                continue;
            }

            if (packet.hasFlag(Packet.FLAG_FIN)) {
                System.out.println("FIN received.");
                sendFinAck(socket, datagram.getAddress(), datagram.getPort(), expectedSeq);
                break;
            }

            int seq = packet.sequenceNumber;

            if (seq == expectedSeq) {
                chunks.put(seq, packet.payload);
                lastAck = expectedSeq + 1;
                expectedSeq++;
                System.out.printf("  ← SEQ=%d ✓ (expecting %d)%n", seq, expectedSeq);
            } else {
                System.out.printf("  ✗ SEQ=%d (expected %d) — dropped%n", seq, expectedSeq);
            }

            if (lastAck >= 0) {
                sendAck(socket, datagram.getAddress(), datagram.getPort(), lastAck);
            }
        }

        writeFile(chunks, outputPath);
        System.out.println("File received and saved to " + outputPath);
        socket.close();
    }

    private void sendAck(DatagramSocket socket, InetAddress addr, int port, int ackNumber) throws IOException {
        Packet ack = new Packet();
        ack.ackNumber = ackNumber;
        ack.setFlag(Packet.FLAG_ACK);
        byte[] raw = ack.serialize();
        socket.send(new DatagramPacket(raw, raw.length, addr, port));
    }

    private void sendFinAck(DatagramSocket socket, InetAddress addr, int port, int ackNumber) throws IOException {
        Packet finAck = new Packet();
        finAck.ackNumber = ackNumber;
        finAck.setFlag(Packet.FLAG_FIN);
        finAck.setFlag(Packet.FLAG_ACK);
        byte[] raw = finAck.serialize();
        socket.send(new DatagramPacket(raw, raw.length, addr, port));
        System.out.println("FIN+ACK sent.");
    }

    private void writeFile(TreeMap<Integer, byte[]> chunks, String outputPath) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(outputPath)) {
            for (byte[] chunk : chunks.values()) {
                fos.write(chunk);
            }
        }
    }
}
