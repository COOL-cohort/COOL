package com.nus.cool.core.io.storevector;

import java.nio.ByteBuffer;
import java.util.BitSet;

public class BitVectorInputVector implements InputVector {

    private long[] words;

    private int[] lookupTable;

    private int[] globalIDs = new int[0];

    private int numOfIDs;

    @Override
    public int size() {
        return this.numOfIDs;
    }

    @Override
    public int find(int key) {
        int i = wordIndex(key);
        int j = remainder(key);
        long bits = this.words[i] << (63 - j);
        return (bits < 0) ? (Long.bitCount(bits) + this.lookupTable[i] - 1) : -1;
    }

    @Override
    public int get(int index) {
        if (this.globalIDs.length == 0)
            return 0;
        return this.globalIDs[index];
    }

    @Override
    public boolean hasNext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int next() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skipTo(int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFrom(ByteBuffer buffer) {
        int len = buffer.get() & 0xFF;
        this.words = new long[len];
        this.lookupTable = new int[len];

        this.lookupTable[0] = 0;
        this.words[0] = buffer.getLong();

        for (int i = 1; i < len; i++) {
            this.words[i] = buffer.getLong();
            this.lookupTable[i] = Long.bitCount(this.words[i - 1]) + this.lookupTable[i - 1];
        }
        this.numOfIDs = this.lookupTable[len - 1] + Long.bitCount(this.words[len - 1]);
    }

    private int wordIndex(int i) {
        return i >>> 6;
    }

    private int remainder(int i) {
        return i & (64 - 1);
    }

    private void fillInGlobalIDs() {
        BitSet bs = BitSet.valueOf(this.words);
        this.globalIDs = new int[this.numOfIDs];
        for (int i = bs.nextSetBit(0), j = 0; i >= 0; i = bs.nextSetBit(i + 1))
            this.globalIDs[j++] = i;
    }
}
