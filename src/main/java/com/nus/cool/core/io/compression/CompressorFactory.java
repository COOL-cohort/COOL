package com.nus.cool.core.io.compression;

import com.nus.cool.core.schema.Codec;

/**
 * Product compressor according to codes
 */
public class CompressorFactory {

    public static Compressor newCompressor(Codec codec, Histogram hist) {
        Compressor compressor;
        switch (codec) {
            case INT32:
                compressor = new ZIntCompressor(codec, hist);
                break;
            case LZ4:
                compressor = new LZ4JavaCompressor(hist);
                break;
            default:
                throw new IllegalArgumentException("Unsupported codec: " + codec);
        }
        return compressor;
    }
}
