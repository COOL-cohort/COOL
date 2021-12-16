package com.nus.cool.core.io.compression;

public interface Compressor {

    /**
     * Estimate the maximum size of compressed data
     *
     * @return the maximum size of compressed data
     */
    int maxCompressedLength();

    /**
     * Compress a byte array
     *
     * @param src        the compressed data
     * @param srcOff     the start offset in sec
     * @param srcLen     the number of bytes to compress
     * @param dest       the destination buffer
     * @param destOff    the start offset in dest
     * @param maxDestLen the maximum number of bytes to write in dest
     * @return the compressed size
     */
    int compress(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen);

    /**
     * Compress an integer array
     *
     * @param src        the compressed data
     * @param srcOff     the start offset in sec
     * @param srcLen     the number of bytes to compress
     * @param dest       the destination buffer
     * @param destOff    the start offset in dest
     * @param maxDestLen the maximum number of bytes to write in dest
     * @return the compressed size
     */
    int compress(int[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen);

}
