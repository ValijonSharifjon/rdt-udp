package com.rdtudp.sender;

public class SenderMain {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: SenderMain <host> <port> <file>");
            return;
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String filePath = args[2];

        StopAndWaitSender sender = new StopAndWaitSender(host, port);
        sender.sendFile(filePath);
        sender.close();
    }
}