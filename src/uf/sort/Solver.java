package uf.sort;

import uf.sort.io.FileBackedResult;
import uf.sort.io.IntermediateResultHolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.*;

public class Solver {

    private final int threadsNumber;
    private final ExecutorService executor;

    public Solver(int threadsNumber) {
        this.threadsNumber = threadsNumber;
        this.executor = Executors.newFixedThreadPool(threadsNumber);
    }

    public void solve(File in, File out) throws IOException, InterruptedException, ExecutionException {

        if (in.length() % 4 != 0) throw new IllegalArgumentException("invalid input file");
        System.err.println("Input data size: " + in.length() + " bytes");

        if (!out.createNewFile()) {
            throw new IllegalArgumentException("out file shouldn't exists");
        }

        int chunkSize = 96 * 1024 * 1024;

        if ((long)threadsNumber * chunkSize > Runtime.getRuntime().maxMemory())
            throw new IllegalArgumentException("too many threads");

        int mergeBuffer = chunkSize / 3;

        IntermediateResultHolder sortResultHolder;
        if (threadsNumber * chunkSize > in.length() * 2) {
            sortResultHolder = new IntermediateResultHolder.InMemory();
        } else {
            sortResultHolder = new IntermediateResultHolder.InTmpFile(mergeBuffer);
        }

        IntermediateResultHolder.InFile inputHolder = new IntermediateResultHolder.InFile(in, chunkSize);
        FileBackedResult input = (FileBackedResult)inputHolder.wrap();

        int chunksNumber = input.chunksNumber(chunkSize);
        System.err.println("Chunks number: " + chunksNumber);

        IntermediateResultHolder outputHolder = new IntermediateResultHolder.InFile(out, mergeBuffer);
        IntermediateResultHolder.IntermediateResult output = null;

        System.err.println("Sorting...");
        long sortStart = System.currentTimeMillis();
        try {

            Iterator<IntermediateResultHolder.IntermediateResult> chunks = input.split(chunkSize);
            int minSortSplitSize = chunkSize / threadsNumber;

            if (chunksNumber == 1) {
                IntermediateResultHolder.IntermediateResult chunk = chunks.next();
                FutureResult futureResult = sortChunk(chunk, minSortSplitSize, outputHolder);
                output = futureResult.get();
                chunk.close();
            } else if (chunksNumber > 0) {
                MergingFutureResult finalMergingFutureResult = new MergingFutureResult(chunksNumber, sortResultHolder, outputHolder);
                while (chunks.hasNext()) {
                    IntermediateResultHolder.IntermediateResult chunk = chunks.next();
                    FutureResult futureResult = sortChunk(chunk, minSortSplitSize, sortResultHolder);
                    IntermediateResultHolder.IntermediateResult result = futureResult.get();
                    finalMergingFutureResult.addPart(result);
                    chunk.close();
                }

                output = finalMergingFutureResult.get();
            }
        } finally {
            input.close();
            if (output != null) output.close();
        }

        long sortEnd = System.currentTimeMillis();
        System.err.println("Sort complete in " + (sortEnd - sortStart) / 1000 + " seconds.");
    }

    void close () {
        executor.shutdown();
    }

    private FutureResult sortChunk(IntermediateResultHolder.IntermediateResult chunk, int minSortSplitSize, IntermediateResultHolder resultHolder) throws IOException {
        int parts = 1;
        int partSize = (int)chunk.size();
        while (partSize > minSortSplitSize) {
            parts *= 2;
            partSize /= 2;
        }
        FutureResult result;
        if (parts == 1) {
            result = new SingleFutureResult(resultHolder);
        } else {
            result = new MergingFutureResult(parts, new IntermediateResultHolder.InMemory(), resultHolder);
        }
        executor.submit(new Sort(chunk, minSortSplitSize, result));
        return result;
    }

    private class Sort implements Runnable {

        private final IntermediateResultHolder.IntermediateResult source;
        private final int minSplitSize;
        private final FutureResult futureResult;

        public Sort(IntermediateResultHolder.IntermediateResult source, int minSplitSize, FutureResult futureResult) {
            this.source = source;
            this.minSplitSize = minSplitSize;
            this.futureResult = futureResult;
        }

        @Override
        public void run() {
            try {
                if (source.size() <= minSplitSize) {
                    int[] data = source.data();
                    Arrays.sort(data);
                    IntermediateResultHolder resultHolder = new IntermediateResultHolder.InMemory();
                    IntermediateResultHolder.IntermediateResult result = resultHolder.hold(data);
                    futureResult.addPart(result);
                } else {
                    IntermediateResultHolder.Tuple tuple = source.split();
                    executor.submit(new Sort(tuple._1, minSplitSize, futureResult));
                    executor.submit(new Sort(tuple._2, minSplitSize, futureResult));
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    source.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static class Merge implements Runnable {

        private final IntermediateResultHolder.IntermediateResult r1;
        private final IntermediateResultHolder.IntermediateResult r2;

        private final IntermediateResultHolder resultHolder;
        private final MergingFutureResult mergingFutureResult;

        public Merge(IntermediateResultHolder.IntermediateResult r1, IntermediateResultHolder.IntermediateResult r2, IntermediateResultHolder resultHolder, MergingFutureResult mergingFutureResult) {
            this.r1 = r1;
            this.r2 = r2;
            this.resultHolder = resultHolder;
            this.mergingFutureResult = mergingFutureResult;
        }

        @Override
        public void run() {
            try {
                IntermediateResultHolder.IntermediateResult result;
                try {
                    result = resultHolder.hold(new MergingIterator(r1.iterate(), r2.iterate()), r1.size() + r2.size());
                } finally {
                    r1.close();
                    r2.close();
                }
                mergingFutureResult.addMergeResult(result);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class MergingIterator implements Iterator<Integer> {

        private Integer v1, v2;

        private final Iterator<Integer> i1, i2;

        public MergingIterator(Iterator<Integer> i1, Iterator<Integer> i2) {
            this.i1 = i1;
            if (i1.hasNext()) v1 = i1.next();
            this.i2 = i2;
            if (i2.hasNext()) v2 = i2.next();
        }

        @Override
        public boolean hasNext() {
            return v1 != null || v2 != null;
        }

        @Override
        public Integer next() {
            if (v1 == null) {
                if (v2 == null) throw new NoSuchElementException();
                Integer result = v2;
                v2 = i2.hasNext() ? i2.next() : null;
                return result;
            }
            if (v2 == null) {
                Integer result = v1;
                v1 = i1.hasNext() ? i1.next() : null;
                return result;
            }
            Integer result;
            if (v1 <= v2) {
                result = v1;
                v1 = i1.hasNext() ? i1.next() : null;
            } else {
                result = v2;
                v2 = i2.hasNext() ? i2.next() : null;
            }
            return result;
        }

        @Override
        public void remove() {

        }
    }

    private interface FutureResult {
        void addPart(IntermediateResultHolder.IntermediateResult part) throws IOException;
        IntermediateResultHolder.IntermediateResult get() throws InterruptedException;
    }

    private class SingleFutureResult implements  FutureResult {

        private final BlockingQueue<IntermediateResultHolder.IntermediateResult> result = new LinkedBlockingQueue<IntermediateResultHolder.IntermediateResult>();
        private final IntermediateResultHolder resultHolder;

        public SingleFutureResult(IntermediateResultHolder resultHolder) {
            this.resultHolder = resultHolder;
        }

        @Override
        public void addPart(IntermediateResultHolder.IntermediateResult part) throws IOException {
            result.add(resultHolder.hold(part.iterate(), part.size()));
        }

        @Override
        public IntermediateResultHolder.IntermediateResult get() throws InterruptedException {
            return result.take();
        }
    }

    private class MergingFutureResult implements FutureResult {
        private int parts;
        private final LinkedList<IntermediateResultHolder.IntermediateResult> completionQueue;
        private final IntermediateResultHolder intermediateResultHolder;
        private final IntermediateResultHolder mergeResultHolder;

        public MergingFutureResult(int parts, IntermediateResultHolder intermediateResultHolder, IntermediateResultHolder mergeResultHolder) {
            this.parts = parts;
            this.intermediateResultHolder = intermediateResultHolder;
            this.completionQueue = new LinkedList<IntermediateResultHolder.IntermediateResult>();
            this.mergeResultHolder = mergeResultHolder;
        }

        @Override
        public void addPart(IntermediateResultHolder.IntermediateResult part) {
            synchronized (completionQueue) {
                completionQueue.add(part);
                merge();
            }
        }

        public void addMergeResult(IntermediateResultHolder.IntermediateResult result) {
            synchronized (completionQueue) {
                parts--;
                completionQueue.add(result);
                merge();
                completionQueue.notifyAll();
            }
        }

        private void merge() {
            while (completionQueue.size() > 1) {
                IntermediateResultHolder.IntermediateResult r1 = completionQueue.poll();
                IntermediateResultHolder.IntermediateResult r2 = completionQueue.poll();
                if (parts > 2) {
                    executor.submit(new Merge(r1, r2, intermediateResultHolder, this));
                } else if (parts == 2) {
                    executor.submit(new Merge(r1, r2, mergeResultHolder, this));
                } else {
                    throw new IllegalStateException();
                }
            }
        }

        @Override
        public IntermediateResultHolder.IntermediateResult get() throws InterruptedException {
            synchronized (completionQueue) {
                while (parts != 1) completionQueue.wait();
                return completionQueue.poll();
            }
        }
    }

}
