package com.rdtudp.protocol;

import java.util.Random;

public class Corruptor {
    public enum Mode {
        BIT_FLIP, 
        DROP
    }

    private final Mode mode;
    private final double lossRate;
    private final Random random;

    public Corruptor(Mode mode, double lossRate) {
        if (lossRate < 0.0 || lossRate > 1.0) {
            throw new IllegalArgumentException("lossRate должен быть от 0.0 до 1.0");
        }
        this.mode = mode;
        this.lossRate = lossRate;
        this.random = new Random();
    }

    public byte[] apply(byte[] raw) {
        if (random.nextDouble() >= lossRate) {
            return raw;
        }

        switch (mode) {
            case DROP:
                return null;

            case BIT_FLIP:
                return flipRandomBit(raw);    
            default:
                return raw;
        }
    }

    private byte[] flipRandomBit(byte[] raw) {
        if (raw.length <= Packet.HEADER_SIZE) {
            return raw;
        }

        byte[] corrupted = raw.clone();

        int payloadStart = Packet.HEADER_SIZE;
        int payloadEnd = raw.length;
        int targetByte = payloadStart + random.nextInt(payloadEnd - payloadStart);

        corrupted[targetByte] ^= (byte)(1 << random.nextInt(8));

        System.out.printf("    ⚡ BIT_FLIP: byte[%d] corrupted%n", targetByte);
        return corrupted;
    }
}
