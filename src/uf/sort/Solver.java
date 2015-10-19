package uf.sort;

import uf.sort.io.FileBackedResult;
import uf.sort.io.IntermediateResultHolder;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static uf.sort.io.IntermediateResultHolder.IntermediateResult;
import static uf.sort.io.IntermediateResultHolder.Tuple;

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
        IntermediateResult output = null;

        System.err.println("Sorting...");
        long sortStart = System.currentTimeMillis();
        try {

            Iterator<IntermediateResult> chunks = input.split(chunkSize);
            int minSortSplitSize = chunkSize / threadsNumber;

            if (chunksNumber == 1) {
                IntermediateResult chunk = chunks.next();
                FutureResult futureResult = sortChunk(chunk, minSortSplitSize, outputHolder);
                output = futureResult.get();
                chunk.close();
            } else if (chunksNumber > 0) {
                MergingFutureResult finalMergingFutureResult = new MergingFutureResult(chunksNumber, sortResultHolder, outputHolder);
                while (chunks.hasNext()) {
                    IntermediateResult chunk = chunks.next();
                    FutureResult futureResult = sortChunk(chunk, minSortSplitSize, sortResultHolder);
                    IntermediateResult result = futureResult.get();
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

    private FutureResult sortChunk(IntermediateResult chunk, int minSortSplitSize, IntermediateResultHolder resultHolder) throws IOException {
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

        private final IntermediateResult source;
        private final int minSplitSize;
        private final FutureResult futureResult;

        public Sort(IntermediateResult source, int minSplitSize, FutureResult futureResult) {
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
                    IntermediateResult result = resultHolder.hold(data);
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

        private final int mergeLevel;

        private final Tuple source;

        private final IntermediateResultHolder resultHolder;
        private final MergingFutureResult mergingFutureResult;

        public Merge(int mergeLevel, Tuple source,
                     IntermediateResultHolder resultHolder, MergingFutureResult mergingFutureResult) {
            this.mergeLevel = mergeLevel;
            this.source = source;
            this.resultHolder = resultHolder;
            this.mergingFutureResult = mergingFutureResult;
        }

        @Override
        public void run() {
            try {
                IntermediateResult result;
                try {
                    MergingIterator merged = new MergingIterator(source._1.iterate(), source._2.iterate());
                    result = resultHolder.hold(merged, source._1.size() + source._2.size());
                } finally {
                    source._1.close();
                    source._2.close();
                }
                mergingFutureResult.addMergeResult(mergeLevel, result);
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
        void addPart(IntermediateResult part) throws IOException;
        IntermediateResult get() throws InterruptedException;
    }

    private class SingleFutureResult implements  FutureResult {

        private final BlockingQueue<IntermediateResult> result = new LinkedBlockingQueue<IntermediateResult>();
        private final IntermediateResultHolder resultHolder;

        public SingleFutureResult(IntermediateResultHolder resultHolder) {
            this.resultHolder = resultHolder;
        }

        @Override
        public void addPart(IntermediateResult part) throws IOException {
            result.add(resultHolder.hold(part.iterate(), part.size()));
        }

        @Override
        public IntermediateResult get() throws InterruptedException {
            return result.take();
        }
    }

    private class MergingFutureResult implements FutureResult {
        private int parts;
        private final SortedMap<Integer, IntermediateResult> completionQueue;
        private final IntermediateResultHolder intermediateResultHolder;
        private final IntermediateResultHolder mergeResultHolder;

        public MergingFutureResult(int parts, IntermediateResultHolder intermediateResultHolder, IntermediateResultHolder mergeResultHolder) {
            this.parts = parts;
            this.intermediateResultHolder = intermediateResultHolder;
            this.completionQueue = new TreeMap<Integer, IntermediateResult>();
            this.mergeResultHolder = mergeResultHolder;
        }

        @Override
        public void addPart(IntermediateResult part) {
            synchronized (completionQueue) {
                merge(0, part);
            }
        }

        public void addMergeResult(Integer level, IntermediateResult result) {
            synchronized (completionQueue) {
                parts--;
                merge(level, result);
                completionQueue.notifyAll();
            }
        }

        private void merge(Integer level, IntermediateResult part) {
            if (completionQueue.isEmpty()) {
                completionQueue.put(level, part);
            } else if (parts > 2) {
                IntermediateResult sameLevelResult = completionQueue.remove(level);
                if (sameLevelResult != null) {
                    Tuple pair = new Tuple(sameLevelResult, part);
                    executor.submit(new Merge(level + 1, pair, intermediateResultHolder, this));
                } else {
                    completionQueue.put(level, part);
                }
                if (completionQueue.size() == parts){
                    while (completionQueue.size() > 2) {
                        int key1 = completionQueue.firstKey();
                        IntermediateResult r1 = completionQueue.remove(key1);
                        int key2 = completionQueue.firstKey();
                        IntermediateResult r2 = completionQueue.remove(key2);
                        Tuple pair = new Tuple(r1, r2);
                        executor.submit(new Merge(Math.max(key1, key2) + 1, pair, intermediateResultHolder, this));
                    }
                }
            } else if (parts == 2) {
                IntermediateResult result = completionQueue.remove(completionQueue.firstKey());
                Tuple pair = new Tuple(result, part);
                executor.submit(new Merge(level + 1, pair, mergeResultHolder, this));
            }
        }

        @Override
        public IntermediateResult get() throws InterruptedException {
            synchronized (completionQueue) {
                while (parts != 1) completionQueue.wait();
                return completionQueue.remove(completionQueue.firstKey());
            }
        }
    }

}
