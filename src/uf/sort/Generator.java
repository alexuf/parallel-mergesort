package uf.sort;

import uf.sort.io.IntermediateResultHolder;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

public class Generator {

    public void gen(File in, final long length) throws IOException {

        System.err.print("Generating input dataset... ");
        final Random rnd = new Random(System.currentTimeMillis());

        Iterator<Integer> desc = new Iterator<Integer>() {

            long produced = 0;

            @Override
            public boolean hasNext() {
                return produced < length / 4;
            }

            @Override
            public Integer next() {
                produced++;
                return rnd.nextInt();
            }

            @Override
            public void remove() {

            }
        };

        long freeMemory = Runtime.getRuntime().freeMemory();
        if (freeMemory > Integer.MAX_VALUE) freeMemory = Integer.MAX_VALUE;
        int writeBuffer = (int)(freeMemory - freeMemory % 4);
        System.err.println("Write buffer: " + writeBuffer + " bytes");

        IntermediateResultHolder resultHolder = new IntermediateResultHolder.InFile(in, writeBuffer);
        try {
            resultHolder.hold(desc, length);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.err.println("done.");
    }
}
