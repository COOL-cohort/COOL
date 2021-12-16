package com.nus.cool.core.io.storevector;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

public class ZIntBitInputVector implements ZIntStore, InputVector {

    private final LongBuffer bitPack;

    private final int capacity;
    private final int bitWidth;
    private final int noValPerPack;
    private final long mask;

    private int pos;
    private long curPack;
    private int packOffset;

    private ZIntBitInputVector(LongBuffer buffer, int capacity, int bitWidth) {
        this.capacity = capacity;
        this.bitWidth = bitWidth;
        this.noValPerPack = 64 / bitWidth;
        this.mask = (bitWidth == 64) ? -1 : (1L << bitWidth) - 1;
        this.bitPack = buffer;
    }

    public static ZIntBitInputVector load(ByteBuffer buffer) {
        int capacity = buffer.getInt();
        int width = buffer.getInt();
        int size = getNumOfBytes(capacity, width);
        int oldLimit = buffer.limit();
        buffer.limit(buffer.position() + size - 8);
        LongBuffer tmpBuffer = buffer.asLongBuffer();
        buffer.position(buffer.position() + size - 8);
        buffer.limit(oldLimit);
        return new ZIntBitInputVector(tmpBuffer, capacity, width);
    }

    private static int getNumOfBytes(int num, int width) {
        int i = 64 / width;
        int size = (num - 1) / i + 2;
        return size << 3;
    }

    @Override
    public int size() {
        return this.capacity;
    }

    @Override
    public int find(int key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int get(int index) {
        if (index >= this.capacity)
            throw new IndexOutOfBoundsException();
        int offset = 64 - (1 + index % this.noValPerPack) * this.bitWidth;
        long pack = getPack(index);
        long val = ((pack >>> offset) & this.mask);
        return (int) val;
    }

    @Override
    public boolean hasNext() {
        return this.pos < this.capacity;
    }

    @Override
    public int next() {
        return (int) nextLong();
    }

    @Override
    public void skipTo(int pos) {
        if (pos >= this.capacity)
            throw new IndexOutOfBoundsException();
        this.pos = pos;
        this.packOffset = 64 - (pos % this.noValPerPack) * this.bitWidth;
        this.bitPack.position(pos / this.noValPerPack);
        this.curPack = this.bitPack.get();
    }

    @Override
    public void readFrom(ByteBuffer buffer) {
        throw new UnsupportedOperationException();
    }

    private long getPack(int pos) {
        int idx = pos / this.noValPerPack;
        return this.bitPack.get(idx);
    }

    private long nextLong() {
        if (this.packOffset < this.bitWidth) {
            this.curPack = this.bitPack.get();
            this.packOffset = 64;
        }
        this.pos++;
        this.packOffset -= this.bitWidth;
        return ((this.curPack >>> this.packOffset) & this.mask);
    }
}
