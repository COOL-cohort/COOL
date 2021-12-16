package com.nus.cool.core.io.compression;

import com.nus.cool.core.io.Output;
import com.nus.cool.core.schema.Codec;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Utility class for writing a compressed integer vector into
 * disk.
 */
public class OutputCompressor implements Output {

    private DataType dataType;
    private int[] vec;
    private byte[] strVec;
    private int off;
    private int len;
    private Histogram hist;

    /**
     * @param h   histogram of compress data
     * @param vec compress data
     * @param off the start offset in the data
     * @param len the number of bytes read
     */
    public void reset(Histogram h, int[] vec, int off, int len) {
        this.hist = h;
        this.vec = vec;
        this.off = off;
        this.len = len;
        this.dataType = DataType.INTEGER;
    }

    /**
     * @param h histogram of compress data
     * @param vec compress data
     * @param off the start offset in the data
     * @param len the number of bytes read
     */
    public void reset(Histogram h, byte[] vec, int off, int len) {
        this.hist = h;
        this.strVec = vec;
        this.off = off;
        this.len = len;
        this.dataType = DataType.STRING;
    }

    @Override
    public int writeTo(DataOutput out) throws IOException {
        int bytesWritten = 0;
        Codec codec = CompressorAdviser.advise(this.hist);
        Compressor compressor = CompressorFactory.newCompressor(codec, this.hist);
        int maxLen = compressor.maxCompressedLength();
        byte[] compressed = new byte[maxLen];
        int compressLen = this.dataType == DataType.INTEGER ?
                compressor.compress(this.vec, this.off, this.len, compressed, 0, maxLen)
                : compressor.compress(this.strVec, this.off, this.len, compressed, 0, maxLen);
        // Write codec
        out.writeByte(codec.ordinal());
        bytesWritten++;
        // Write compressed data
        out.write(compressed, 0, compressLen);
        bytesWritten += compressLen;
        return bytesWritten;
    }

    private enum DataType {INTEGER, STRING}
}
