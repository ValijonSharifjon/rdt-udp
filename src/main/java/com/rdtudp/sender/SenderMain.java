package com.rdtudp.sender;

import com.rdtudp.protocol.Corruptor;

public class SenderMain {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: SenderMain <host> <port> <file>");
            return;
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String filePath = args[2];

        Corruptor.Mode mode = null;
        double lossRate = 0.1;

        for (int i = 3; i < args.length; ++i) {
            if (args[i].equals("--corrupt") && i + 1 < args.length) {
                mode = Corruptor.Mode.valueOf(args[++i]);
            }
            if (args[i].equals("--loss-rate") && i + 1 < args.length) {
                lossRate = Double.parseDouble(args[++i]);
            }
        }

        StopAndWaitSender sender = new StopAndWaitSender(host, port);

        if (mode != null) {
            sender.setCorruptor(new Corruptor(mode, lossRate));
            System.out.printf("Corruptor enabled: mode=%s, loss-rate=%.0f%%%n",
                    mode, lossRate * 100);
        }

        sender.sendFile(filePath);
        sender.close();
    }
}