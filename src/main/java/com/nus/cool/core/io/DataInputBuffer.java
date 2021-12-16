package com.nus.cool.core.io;

import java.io.DataInputStream;

public class DataInputBuffer extends DataInputStream {

    private Buffer buffer;

    public DataInputBuffer() {
        this(new Buffer());
    }

    public DataInputBuffer(Buffer buffer) {
        super(buffer);
        this.buffer = buffer;
    }

    /**
     * Resets te data that the buffer reads
     *
     * @param input  the input stream
     * @param length number of bytes to read
     */
    public void reset(byte[] input, int length) {
        this.buffer.reset(input, 0, length);
    }

    /**
     * Resets te data that the buffer reads
     *
     * @param input the input stream
     */
    public void reset(DataOutputBuffer input) {
        reset(input.getData(), input.size());
    }

    public byte[] getData() {
        return this.buffer.getData();
    }

    public int getPosition() {
        return this.buffer.getPosition();
    }

    public int getLength() {
        return this.buffer.getLength();
    }

    private static class Buffer extends FastInputStream {

        public Buffer() {
            super(new byte[]{});
        }

        public void reset(byte[] input, int start, int length) {
            this.buffer = input;
            this.count = start + length;
            this.mark = start;
            this.pos = start;
        }

        public byte[] getData() {
            return this.buffer;
        }

        public int getPosition() {
            return this.pos;
        }

        public int getLength() {
            return this.count;
        }
    }
}
