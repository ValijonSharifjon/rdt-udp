package com.rdtudp.receiver;

public class ReceiverMain {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: ReceiverMain <port> <outputFile>");
            return;
        }

        int port = Integer.parseInt(args[0]);
        String outputPath = args[1];

        StopAndWaitReceiver receiver = new StopAndWaitReceiver(port);
        receiver.receive(outputPath);
    }
}