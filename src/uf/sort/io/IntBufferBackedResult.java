package uf.sort.io;

import java.nio.IntBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class IntBufferBackedResult implements IntermediateResultHolder.IntermediateResult {

    private class IntBufferBackedIterator implements Iterator<Integer> {

        private final IntBuffer buf;

        public IntBufferBackedIterator(IntBuffer buf) {
            this.buf = buf;
        }

        @Override
        public boolean hasNext() {
            return buf.hasRemaining();
        }

        @Override
        public Integer next() {
            if (!hasNext()) throw new NoSuchElementException();
            return buf.get();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private volatile IntBuffer buf;

    public IntBufferBackedResult(IntBuffer buf) {
        this.buf = buf;
    }

    public IntBufferBackedResult(int[] data) {
        this(IntBuffer.wrap(data));
    }

    @Override
    public IntermediateResultHolder.Tuple split() {

        int half = buf.limit() / 2;

        IntBuffer first = this.buf.duplicate();
        first.limit(half);

        IntBuffer second = this.buf.duplicate();
        second.position(half);

        return new IntermediateResultHolder.Tuple(
                new IntBufferBackedResult(first.slice()),
                new IntBufferBackedResult(second.slice())
        );
    }

    @Override
    public long size() {
        return buf.limit() * 4;
    }

    @Override
    public int[] data() {
        int[] data = new int[buf.limit()];
        buf.duplicate().get(data);
        return data;
    }

    @Override
    public Iterator<Integer> iterate() {
        if (buf == null) throw new IllegalStateException("result closed");
        return new IntBufferBackedIterator(buf.duplicate());
    }

    @Override
    public void close() {
        buf = null;
        Runtime.getRuntime().gc();
    }
}
