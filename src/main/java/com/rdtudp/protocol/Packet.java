package com.rdtudp.protocol;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class Packet {
    public static final int HEADER_SIZE = 16;
    public static final int MAX_PAYLOAD = 1024;
    public static final int   MAX_PACKET  = HEADER_SIZE + MAX_PAYLOAD;

    public static final short FLAG_SYN = (short)(1 << 0);
    public static final short FLAG_ACK = (short)(1 << 1);
    public static final short FLAG_FIN = (short)(1 << 2);

    public int sequenceNumber;
    public int ackNumber;
    public short  flags;
    public byte[] payload;

    public Packet() {}

    public byte[] serialize() {
        int payloadLen = (payload != null) ? payload.length : 0;

        ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE + payloadLen);

        buf.putInt(sequenceNumber);
        buf.putInt(ackNumber);
        buf.putShort(flags);
        buf.putShort((short)payloadLen);
        buf.putInt(0);

        if (payloadLen > 0) {
            buf.put(payload);
        }

        byte[] raw = buf.array();

        int checkSum = computeChecksum(raw);
        ByteBuffer.wrap(raw).putInt(12, checkSum);

        return raw;
    }

    public static Packet deserialize(byte[] raw) throws PacketCorruptedException {
        if (raw.length < HEADER_SIZE) {
            throw new PacketCorruptedException("Packet too short: " + raw.length + " bytes");
        }

        ByteBuffer buf = ByteBuffer.wrap(raw);

        int seqNum = buf.getInt();
        int ackNum = buf.getInt();
        short flgs = buf.getShort();
        short dataLen = buf.getShort();
        int storedChecksum = buf.getInt();

        byte[] copy = raw.clone();
        ByteBuffer.wrap(copy).putInt(12, 0);
        int computed = computeChecksum(copy);

        if (computed != storedChecksum) {
            throw new PacketCorruptedException(
                String.format("CRC32 mismatch: expected %d, got %d", computed, storedChecksum)
            );
        }

        Packet p = new Packet();
        p.sequenceNumber = seqNum;
        p.ackNumber = ackNum;
        p.flags = flgs;

        if (dataLen > 0) {
            p.payload = new byte[dataLen];
            buf.get(p.payload);
        }

        return p;
    }

    public boolean hasFlag(short flag) {
        return (flags & flag) != 0;
    }

    public void setFlag(short flag) {
        flags |= flag;
    }

    private static int computeChecksum(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }

    @Override
    public String toString() {
        return String.format("Packet[seq=%d ack=%d flags=%s len=%d]",
            sequenceNumber, ackNumber, flagsToString(),
            payload != null ? payload.length : 0);
    }

    private String flagsToString() {
        StringBuilder sb = new StringBuilder();
        if (hasFlag(FLAG_SYN)) sb.append("SYN ");
        if (hasFlag(FLAG_ACK)) sb.append("ACK ");
        if (hasFlag(FLAG_FIN)) sb.append("FIN ");
        return sb.length() > 0 ? sb.toString().trim() : "NONE";
    }
}
