package uf.sort;

import java.io.*;
import java.util.concurrent.*;

public class Main {

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {

        if (args.length < 3) {
            System.err.println("Usage: <threads number> <input file> <output file> [mode]");
            System.exit(1);
        }

        int threadsNumber;
        try {
            threadsNumber = Integer.parseInt(args[0]);
            if (threadsNumber <= 0) {
                threadsNumber = Runtime.getRuntime().availableProcessors();
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Thread number " + args[0] + " must be an integer.");
        }

        System.err.println("Threads number: " + threadsNumber);

        File in = new File(args[1]);

        File out = new File(args[2]);

        String mode = "solve";
        if (args.length > 3) {
            mode = args[3].trim();
        }

        if (mode.equalsIgnoreCase("generate")) {
            long size = 1024L*1024*1024;
            if (args.length > 4) size = Long.parseLong(args[4]);
            if (size < 0) throw new IllegalArgumentException("illegal size");
            if (size % 4 != 0) throw new IllegalArgumentException("illegal size");
            in.delete();
            in.createNewFile();
            new Generator().gen(in, size);
        } else if (mode.equalsIgnoreCase("validate")) {
            new Validator().validate(in, out);
        } else if (mode.equalsIgnoreCase("solve")) {
            Solver solver = new Solver(threadsNumber);
            try {
                solver.solve(in, out);
            } finally {
                solver.close();
            }
        } else {
            throw new IllegalArgumentException("Illegal mode " + mode);
        }
    }
}
