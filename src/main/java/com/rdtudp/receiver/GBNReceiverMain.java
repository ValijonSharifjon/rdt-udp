package com.rdtudp.receiver;

public class GBNReceiverMain {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: GBNReceiverMain <port> <outputFile>");
            return;
        }
        new GBNReceiver(Integer.parseInt(args[0])).receive(args[1]);
    }
}
