package com.nus.cool.core.io.storevector;

import com.google.common.primitives.Shorts;
import com.nus.cool.core.util.ShortBuffers;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;

public class ZInt16Store implements ZIntStore, InputVector {

    private int count;

    private ShortBuffer buffer;

    public ZInt16Store(int count) {
        this.count = count;
    }

    public static ZIntStore load(ByteBuffer buffer, int n) {
        ZIntStore store = new ZInt16Store(n);
        store.readFrom(buffer);
        return store;
    }

    @Override
    public int size() {
        return this.count;
    }

    @Override
    public int find(int key) {
        if (key > Short.MAX_VALUE || key < 0)
            return -1;
        return ShortBuffers.binarySearchUnsigned(this.buffer, 0, this.buffer.limit(), (short) key);
    }

    @Override
    public int get(int index) {
        return (this.buffer.get(index) & 0xFFFF);
    }

    @Override
    public boolean hasNext() {
        return this.buffer.hasRemaining();
    }

    @Override
    public int next() {
        return (this.buffer.get() & 0xFFFF);
    }

    @Override
    public void skipTo(int pos) {
        this.buffer.position(pos);
    }

    @Override
    public void readFrom(ByteBuffer buffer) {
        int limit = buffer.limit();
        int newLimit = buffer.position() + this.count * Shorts.BYTES;
        buffer.limit(newLimit);
        this.buffer = buffer.asShortBuffer();
        buffer.position(newLimit);
        buffer.limit(limit);
    }
}
