package com.nus.cool.core.io.compression;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * LZ4Compressor for compressing string values
 * <p>
 * The compressed data layout
 * ---------------------------------------
 * | z len | raw len | compressed values |
 * ---------------------------------------
 */
public class LZ4JavaCompressor implements Compressor {

    /**
     * Bytes number for z len and raw len
     */
    public static final int HEADACC = 4 + 4;

    /**
     * Maximum size of compressed data
     */
    private int maxLen;

    /**
     * LZ4 compressor
     */
    private LZ4Compressor lz4;

    public LZ4JavaCompressor(Histogram hist) {
        this.lz4 = LZ4Factory.fastestInstance().fastCompressor();
        this.maxLen = lz4.maxCompressedLength(hist.getRawSize()) + HEADACC;
    }

    @Override
    public int maxCompressedLength() {
        return this.maxLen;
    }

    @Override
    public int compress(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen) {
        ByteBuffer buffer = ByteBuffer.wrap(dest, destOff, maxDestLen).order(ByteOrder.nativeOrder());
        int zLen = this.lz4.compress(src, srcOff, srcLen, dest, destOff + HEADACC, maxDestLen - HEADACC);
        // write z len and raw len for decompressing
        buffer.putInt(zLen);
        buffer.putInt(srcLen);
        return HEADACC + zLen;
    }

    @Override
    public int compress(int[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen) {
        throw new UnsupportedOperationException();
    }

}
