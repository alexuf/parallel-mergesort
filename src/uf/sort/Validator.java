package uf.sort;

import uf.sort.io.IntermediateResultHolder;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static uf.sort.io.IntermediateResultHolder.IntermediateResult;

public class Validator {


    public void validate(final File in, File out) throws IOException {

        System.err.println("Validating result... ");

        long freeMemory = Runtime.getRuntime().freeMemory();
        if (freeMemory > Integer.MAX_VALUE) freeMemory = Integer.MAX_VALUE;
        int readBuffer = (int)(freeMemory - freeMemory % 4);
        System.err.println("Read buffer: " + readBuffer + " bytes");

        if (in.length() != out.length()) throw new RuntimeException("invalid output size");

        IntermediateResult result = new IntermediateResultHolder.InFile(out, readBuffer).wrap();

        try {
            int prev = Integer.MIN_VALUE;
            Iterator<Integer> data = result.iterate();
            while (data.hasNext()) {
                int value = data.next();
                if (value < prev) throw new RuntimeException("invalid order");
                prev = value;
            }
        } finally {
            result.close();
        }


        System.err.println("done.");
    }
}
