package uf.sort.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

public interface IntermediateResultHolder  {

    class Tuple {
        public final IntermediateResult _1;
        public final IntermediateResult _2;

        public Tuple(IntermediateResult _1, IntermediateResult _2) {
            this._1 = _1;
            this._2 = _2;
        }
    }

    interface IntermediateResult {

        Tuple split();

        long size() throws IOException;

        int[] data();

        Iterator<Integer> iterate() throws IOException;

        void close() throws IOException;
    }

    IntermediateResult hold(int[] data) throws IOException;

    IntermediateResult hold(Iterator<Integer> data, long size) throws IOException;

    class InMemory implements IntermediateResultHolder {

        @Override
        public IntermediateResult hold(int[] data) throws IOException {
            return new IntBufferBackedResult(data);
        }

        @Override
        public IntermediateResult hold(Iterator<Integer> iterator, long size) throws IOException {
            if (size > Integer.MAX_VALUE) throw new IllegalArgumentException("");
            int[] data = new int[(int)size / 4];
            for (int i = 0; i < size / 4; i++) {
                data[i] = iterator.next();
            }
            return new IntBufferBackedResult(data);
        }
    }

    class InFile implements IntermediateResultHolder {

        private final File file;
        private final int bufferSize;

        public InFile(File file, int bufferSize) {
            this.file = file;
            this.bufferSize = bufferSize;
        }

        @Override
        public IntermediateResult hold(int[] data) throws IOException {
            RandomAccessFile file = new RandomAccessFile(this.file, "rw");
            try {
                ByteBuffer buffer = file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, data.length * 4);
                buffer.order(ByteOrder.BIG_ENDIAN).asIntBuffer().put(data);
            } finally {
                file.close();
            }
            return new FileBackedResult(file, bufferSize);
        }

        @Override
        public IntermediateResult hold(Iterator<Integer> data, long size) throws IOException {

            RandomAccessFile file = new RandomAccessFile(this.file, "rw");
            try {
                FileChannel channel = file.getChannel();

                IntBuffer buffer = IntBuffer.allocate(bufferSize / 4);
                while (data.hasNext()) {
                    buffer.put(data.next());
                    if (buffer.remaining() == 0) {
                        buffer.flip();
                        channel.map(FileChannel.MapMode.READ_WRITE, channel.position(), bufferSize)
                                .order(ByteOrder.BIG_ENDIAN).asIntBuffer().put(buffer);
                        channel.position(channel.position() + bufferSize);
                        buffer = IntBuffer.allocate(bufferSize / 4);
                    }
                }
                buffer.flip();
                channel.map(FileChannel.MapMode.READ_WRITE, channel.position(), buffer.limit() * 4)
                        .order(ByteOrder.BIG_ENDIAN).asIntBuffer().put(buffer);
            } finally {
                file.close();
            }

            return new FileBackedResult(new RandomAccessFile(this.file, "r"), bufferSize);
        }

        public IntermediateResult wrap() throws FileNotFoundException {
            RandomAccessFile file = new RandomAccessFile(this.file, "rw");
            return new FileBackedResult(file, bufferSize);
        }
    }

    class InTmpFile implements IntermediateResultHolder {

        private final int bufferSize;

        public InTmpFile(int bufferSize) {
            this.bufferSize = bufferSize;
        }

        private class TmpFileBackedResult extends FileBackedResult {

            private final File file;

            public TmpFileBackedResult(File file) throws FileNotFoundException {
                super(new RandomAccessFile(file, "r"), bufferSize);
                this.file = file;
            }

            @Override
            public void close() throws IOException {
                super.close();
                file.delete();
            }
        }

        @Override
        public IntermediateResult hold(int[] data) throws IOException {
            File tmpFile = File.createTempFile("sort", null, new File(System.getProperty("user.dir")));
            tmpFile.deleteOnExit();
            RandomAccessFile file = new RandomAccessFile(tmpFile, "rw");
            try {
                ByteBuffer buffer = file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, data.length * 4);
                buffer.order(ByteOrder.BIG_ENDIAN).asIntBuffer().put(data);
            } finally {
                file.close();
            }
            return new TmpFileBackedResult(tmpFile);
        }

        @Override
        public IntermediateResult hold(Iterator<Integer> data, long size) throws IOException {
            File tmpFile = File.createTempFile("sort", null, new File(System.getProperty("user.dir")));
            tmpFile.deleteOnExit();

            RandomAccessFile file = new RandomAccessFile(tmpFile, "rw");
            try {
                FileChannel channel = file.getChannel();

                IntBuffer buffer = IntBuffer.allocate(bufferSize / 4);
                while (data.hasNext()) {
                    buffer.put(data.next());
                    if (buffer.remaining() == 0) {
                        buffer.flip();
                        channel.map(FileChannel.MapMode.READ_WRITE, channel.position(), bufferSize)
                                .order(ByteOrder.BIG_ENDIAN).asIntBuffer().put(buffer);
                        channel.position(channel.position() + bufferSize);
                        buffer = IntBuffer.allocate(bufferSize / 4);
                    }
                }
                buffer.flip();
                channel.map(FileChannel.MapMode.READ_WRITE, channel.position(), buffer.limit() * 4)
                        .order(ByteOrder.BIG_ENDIAN).asIntBuffer().put(buffer);
            } finally {
                file.close();
            }
            return new TmpFileBackedResult(tmpFile);
        }
    }

}
