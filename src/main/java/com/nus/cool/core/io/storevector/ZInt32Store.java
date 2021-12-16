package com.nus.cool.core.io.storevector;

import com.google.common.primitives.Ints;
import com.nus.cool.core.util.IntBuffers;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class ZInt32Store implements ZIntStore, InputVector {

    private int count;

    private IntBuffer buffer;

    public ZInt32Store(int count) {
        this.count = count;
    }

    public static ZIntStore load(ByteBuffer buffer, int n) {
        ZIntStore store = new ZInt32Store(n);
        store.readFrom(buffer);
        return store;
    }

    @Override
    public int size() {
        return this.count;
    }

    @Override
    public int find(int key) {
        return IntBuffers.binarySearch(this.buffer, 0, this.buffer.limit(), key);
    }

    @Override
    public int get(int index) {
        return this.buffer.get(index);
    }

    @Override
    public boolean hasNext() {
        return this.buffer.hasRemaining();
    }

    @Override
    public int next() {
        return this.buffer.get();
    }

    @Override
    public void skipTo(int pos) {
        this.buffer.position(pos);
    }

    @Override
    public void readFrom(ByteBuffer buffer) {
        int limit = buffer.limit();
        int newLimit = buffer.position() + this.count * Ints.BYTES;
        buffer.limit(newLimit);
        this.buffer = buffer.asIntBuffer();
        buffer.position(newLimit);
        buffer.limit(limit);
    }
}
