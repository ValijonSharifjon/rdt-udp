package com.rdtudp.protocol;

public class PacketCorruptedException extends Exception {
    public PacketCorruptedException(String message) {
        super(message);
    }
}
