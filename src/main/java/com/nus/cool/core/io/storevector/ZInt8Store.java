package com.nus.cool.core.io.storevector;

import com.nus.cool.core.util.ByteBuffers;

import java.nio.ByteBuffer;

/**
 * Decompress data which stores integers in one byte
 * <p>
 * The data layout is as follows
 * ------------------------------------
 * | count | ZInt compressed integers |
 * ------------------------------------
 */
public class ZInt8Store implements InputVector, ZIntStore {

    /**
     * number of values
     */
    private int count;

    /**
     * compressed data
     */
    private ByteBuffer buffer;

    public ZInt8Store(int count) {
        this.count = count;
    }

    public static ZIntStore load(ByteBuffer buffer, int n) {
        ZIntStore store = new ZInt8Store(n);
        store.readFrom(buffer);
        return store;
    }

    @Override
    public int size() {
        return this.count;
    }

    @Override
    public int find(int key) {
        if (key > Byte.MAX_VALUE || key < 0)
            return -1;
        return ByteBuffers.binarySearchUnsigned(this.buffer, 0, this.buffer.limit(), (byte) key);
    }

    @Override
    public int get(int index) {
        return (this.buffer.get(index) & 0xFF);
    }

    @Override
    public boolean hasNext() {
        return this.buffer.hasRemaining();
    }

    @Override
    public int next() {
        return (this.buffer.get() & 0xFF);
    }

    @Override
    public void skipTo(int pos) {
        this.buffer.position(pos);
    }

    @Override
    public void readFrom(ByteBuffer buffer) {
        int limit = buffer.limit();
        int newLimit = buffer.position() + this.count;
        buffer.limit(newLimit);
        this.buffer = buffer.slice();
        buffer.position(newLimit);
        buffer.limit(limit);
    }
}
