package com.nus.cool.core.io.compression;

import com.nus.cool.core.util.IntegerUtil;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;

/**
 * Compress a list of integers with BitVector encoding. The final BitSet
 * is encoded in native byte order format.
 * <p>
 * Data layout
 * ---------------
 * | len | words |
 * ---------------
 * where len is the number of words. Each word is a 64bit computer word
 */
public class BitVectorCompressor implements Compressor {

    /**
     * Bit vector
     */
    private BitSet bitSet;

    /**
     * Maximum size of compressed data
     */
    private int maxLength;

    public BitVectorCompressor(Histogram hist) {
        int bitLength = IntegerUtil.numOfBits((int) hist.getMax());
        this.bitSet = new BitSet(bitLength);
        this.maxLength = (bitLength >>> 3) + 1;
    }

    @Override
    public int maxCompressedLength() {
        return this.maxLength;
    }

    @Override
    public int compress(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compress(int[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen) {
        for (int i = srcOff; i < srcOff + srcLen; i++)
            this.bitSet.set(src[i]);
        long[] words = this.bitSet.toLongArray();
        ByteBuffer buffer = ByteBuffer.wrap(dest, destOff, maxDestLen);
        buffer.order(ByteOrder.nativeOrder());
        buffer.put((byte) words.length);
        for (long w : words)
            buffer.putLong(w);
        return buffer.position();
    }
}
