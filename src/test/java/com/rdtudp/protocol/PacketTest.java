package com.rdtudp.protocol;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class PacketTest {
    @Test
    void roundTrip_preservesAllFields() throws Exception {
        Packet original = new Packet();
        original.sequenceNumber = 42;
        original.ackNumber = 7;
        original.setFlag(Packet.FLAG_ACK);
        original.payload = new byte[]{10, 20, 30};

        byte[] raw = original.serialize();
        Packet result = Packet.deserialize(raw);

        assertEquals(42, result.sequenceNumber);
        assertEquals(7, result.ackNumber);
        assertTrue(result.hasFlag(Packet.FLAG_ACK));
        assertArrayEquals(new byte[]{10, 20, 30}, result.payload);
    }

    @Test
    void corrupted_throwsException() {
        Packet p = new Packet();
        p.sequenceNumber = 1;
        p.payload = "hello".getBytes();

        byte[] raw = p.serialize();
        raw[17] ^= 0xFF;

        assertThrows(PacketCorruptedException.class, () -> Packet.deserialize(raw));
    }

    @Test
    void ackPacket_sizeIsHeaderOnly() throws Exception {
        Packet p = new Packet();
        p.ackNumber = 1;
        p.setFlag(Packet.FLAG_ACK);

        byte[] raw = p.serialize();

        assertEquals(Packet.HEADER_SIZE, raw.length);
        assertNull(Packet.deserialize(raw).payload);
    }
}
