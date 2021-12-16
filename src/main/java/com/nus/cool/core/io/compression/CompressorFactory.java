package com.nus.cool.core.io.compression;

import com.nus.cool.core.schema.Codec;

/**
 * Product compressor according to codes
 */
public class CompressorFactory {

    public static Compressor newCompressor(Codec codec, Histogram hist) {
        Compressor compressor;
        switch (codec) {
            case INT8:
            case INT16:
            case INT32:
                compressor = new ZIntCompressor(codec, hist);
                break;
            case LZ4:
                compressor = new LZ4JavaCompressor(hist);
                break;
            case BitVector:
                compressor = new BitVectorCompressor(hist);
                break;
            case RLE:
                compressor = new RLECompressor(hist);
                break;
            case INTBit:
                compressor = new ZIntBitCompressor(hist);
                break;
            case Delta:
                compressor = new DeltaCompressor(hist);
                break;
            default:
                throw new IllegalArgumentException("Unsupported codec: " + codec);
        }
        return compressor;
    }
}
