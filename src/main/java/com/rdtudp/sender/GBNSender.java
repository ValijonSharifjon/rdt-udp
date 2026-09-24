package com.rdtudp.sender;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.rdtudp.protocol.Corruptor;
import com.rdtudp.protocol.Packet;
import com.rdtudp.protocol.PacketCorruptedException;

public class GBNSender {
    private static final int WINDOW_SIZE = 4;
    private static final int TIMEOUT_MS  = 500;
    private static final int MAX_RETRIES = 20;

    private final DatagramSocket socket;
    private final InetAddress serverAddress;
    private final int serverPort;
    private Corruptor corruptor;

    private final AtomicInteger base = new AtomicInteger(0);
    private final AtomicInteger nextSeq = new AtomicInteger(0);

    private final ConcurrentHashMap<Integer, byte[]> window = new ConcurrentHashMap<>();

    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    private volatile ScheduledFuture<?> timeoutTask;
    private final AtomicInteger retryCount = new AtomicInteger(0); 

    private volatile boolean transferDone = false;
    private volatile IOException transferError = null;

    public GBNSender(String host, int port) throws IOException {
        this.socket = new DatagramSocket();
        this.socket.setSoTimeout(100);
        this.serverAddress = InetAddress.getByName(host);
        this.serverPort = port;
    }

    public void setCorruptor(Corruptor corruptor) {
        this.corruptor = corruptor;
    }

    public void sendFile(String filePath) throws IOException {
        byte[] fileData = new FileInputStream(filePath).readAllBytes();
        int totalChunks = (int) Math.ceil((double) fileData.length / Packet.MAX_PAYLOAD);

        System.out.printf("Sending: %s (%d bytes, %d packets, window=%d)%n",
        filePath, fileData.length, totalChunks, WINDOW_SIZE);

        Thread ackThread = new Thread(() -> receiveAck(totalChunks));
        ackThread.setDaemon(true);
        ackThread.start();

        while (base.get() < totalChunks) {
            if (transferError != null) {
                throw transferError;
            }

            while(nextSeq.get() < base.get() + WINDOW_SIZE && nextSeq.get() < totalChunks) {
                int seq = nextSeq.get();
                byte[] chunk = extractChunk(fileData, seq);

                Packet packet = new Packet();
                packet.sequenceNumber = seq;
                packet.payload = chunk;
                byte[] raw = packet.serialize();

                byte[] toSend = applyCorruptor(raw);

                window.put(seq, raw);

                if (toSend != null) {
                    sendDatagram(toSend);
                    System.out.printf("→ SEQ=%d%n", seq);
                } else {
                    System.out.printf("→ SEQ=%d [DROPPED]%n", seq);
                }

                if (seq == base.get()) {
                    resetTimer(totalChunks, fileData);
                }

                nextSeq.incrementAndGet();
            }

            try { Thread.sleep(1); } catch (InterruptedException e) { break; }
        }

        cancelTimer();
        sendFin(totalChunks);
        timer.shutdown();

        System.out.printf("Done. Retries: %d%n", retryCount.get());
    }

    private void sendDatagram(byte[] raw) throws IOException {
        socket.send(new DatagramPacket(raw, raw.length, serverAddress, serverPort));
    }

    private byte[] applyCorruptor(byte[] raw) {
        if (corruptor == null) return raw;
        return corruptor.apply(raw);
    }

    private void receiveAck(int totalChunks) {
        byte[] buf = new byte[Packet.MAX_PACKET];

        while (!transferDone && base.get() < totalChunks) {
            try {
                DatagramPacket datagram = new DatagramPacket(buf, buf.length);
                socket.receive(datagram);

                byte[] received = new byte[datagram.getLength()];
                System.arraycopy(buf, 0, received, 0, datagram.getLength());

                Packet ack = Packet.deserialize(received);

                if (ack.hasFlag(Packet.FLAG_ACK)) {
                    int ackedSeq = ack.ackNumber - 1;

                    if (ackedSeq >= base.get()) {
                        for (int i = base.get(); i <= ackedSeq; ++i) {
                            window.remove(i);
                        }

                        System.out.printf("← ACK=%d (base: %d→%d)%n",
                            ack.ackNumber, base.get(), ackedSeq + 1);
                        base.set(ackedSeq + 1);
                        retryCount.set(0);    
                    }
                }
            } catch (SocketTimeoutException e) {
                
            } catch(PacketCorruptedException e) {
                System.out.println("← Corrupted ACK — ignoring");
            } catch (IOException e) {
                if (!transferDone) {
                    transferError = e;
                }
            }
        }
    }

    private void resetTimer(int totalChunks, byte[] fileData) {
        cancelTimer();
        timeoutTask = timer.schedule(
            () -> onTimeout(totalChunks, fileData),
            TIMEOUT_MS,
            TimeUnit.MILLISECONDS
        );
    }

    private void cancelTimer() {
        if (timeoutTask != null && !timeoutTask.isDone()) {
            timeoutTask.cancel(false);
        }
    }

    private void onTimeout(int totalChunks, byte[] fileData) {
        int currentBase    = base.get();
        int currentNextSeq = nextSeq.get();

        if (retryCount.incrementAndGet() > MAX_RETRIES) {
            transferError = new IOException(
                "MAX_RETRIES exceeded for SEQ=" + currentBase
            );
            return;
        }

        System.out.printf("⚠ Timeout! Retransmitting window [%d, %d), attempt %d%n",
                currentBase, currentNextSeq, retryCount.get());

        for (int seq = currentBase; seq < currentNextSeq; seq++) {
            byte[] raw = window.get(seq);
            if (raw != null) {
                try {
                    byte[] toSend = applyCorruptor(raw);
                    if (toSend != null) {
                        sendDatagram(toSend);
                        System.out.printf("  ↺ SEQ=%d%n", seq);
                    }
                } catch (IOException e) {
                    transferError = e;
                    return;
                }
            }
        }

        resetTimer(totalChunks, fileData);
    }

    private void sendFin(int nextSeq) throws IOException {
        Packet fin = new Packet();
        fin.sequenceNumber = nextSeq;
        fin.setFlag(Packet.FLAG_FIN);
        byte[] raw = fin.serialize();

        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
                sendDatagram(raw);
                try {
                Thread.sleep(TIMEOUT_MS);
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private byte[] extractChunk(byte[] fileData, int index) {
        int from = index * Packet.MAX_PAYLOAD;
        int to   = Math.min(from + Packet.MAX_PAYLOAD, fileData.length);
        return java.util.Arrays.copyOfRange(fileData, from, to);
    }

    public void close() {
        transferDone = true;
        socket.close();
    }
}
