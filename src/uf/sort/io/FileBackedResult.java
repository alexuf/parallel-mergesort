package uf.sort.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import static uf.sort.io.IntermediateResultHolder.IntermediateResult;

public class FileBackedResult implements IntermediateResult {

    private final RandomAccessFile file;
    private final int bufferSize;

    public FileBackedResult(RandomAccessFile file, int bufferSize) throws FileNotFoundException {
        this.file = file;
        this.bufferSize = bufferSize;
    }

    @Override
    public IntermediateResultHolder.Tuple split() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long size() throws IOException {
        return file.length();
    }

    @Override
    public Iterator<Integer> iterate() throws IOException {
        return new RandomAccessFileBackedIterator();
    }

    @Override
    public int[] data() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        file.close();
    }

    public int chunksNumber(int chunkSize) throws IOException {

        int chunksNumber = (int) (file.length() / chunkSize);
        if ((long) chunksNumber * chunkSize < file.length()) chunksNumber += 1;
        return chunksNumber;
    }

    public Iterator<IntermediateResult> split(int chunkSize) throws IOException {

        return new FileChunksIterator(chunkSize);
    }
    
    class FileChunksIterator implements Iterator<IntermediateResult> {

        private final int chunkSize;
        private long position = 0;

        public FileChunksIterator(int chunkSize) {
            this.chunkSize = chunkSize;
        }

        @Override
        public boolean hasNext() {
            try {
                return position < file.length();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public IntermediateResult next() {
            try {
                int currentChunkSize;
                if (file.length() - position > chunkSize) {
                    currentChunkSize = chunkSize;
                } else {
                    currentChunkSize = (int)(file.length() - position);
                }
                IntBuffer buffer = file.getChannel()
                        .map(FileChannel.MapMode.READ_ONLY, position, currentChunkSize)
                        .order(ByteOrder.BIG_ENDIAN).asIntBuffer();
                position += currentChunkSize;
                return new IntBufferBackedResult(buffer);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    class RandomAccessFileBackedIterator implements Iterator<Integer> {

        private final Iterator<IntermediateResult> chunks = new FileChunksIterator(bufferSize);
        private Iterator<Integer> chunkIterator;
        private IntermediateResult chunk;

        @Override
        public boolean hasNext() {
            return (chunkIterator != null && chunkIterator.hasNext()) || chunks.hasNext();
        }

        @Override
        public Integer next() {
            if (chunkIterator == null || !chunkIterator.hasNext()) try {
                nextChunk();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return chunkIterator.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private void nextChunk() throws IOException {
            if (chunk != null) {
                chunkIterator = null;
                chunk.close();
            }
            try {
                chunk = chunks.next();
                chunkIterator = chunk.iterate();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
