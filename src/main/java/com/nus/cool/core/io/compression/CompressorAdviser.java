package com.nus.cool.core.io.compression;

import com.nus.cool.core.schema.Codec;
import com.nus.cool.core.schema.CompressType;
import com.nus.cool.core.util.IntegerUtil;

/**
 * Advise codec according to compress type
 * <p>
 * KeyFinger    -> LZ4
 * KeyString    -> INT32
 * KeyHash      -> { BitVector, INT8, INT16, INT32 }
 * Value        -> { RLE, INT8, INT16, INT32, INTBit }
 */
public class CompressorAdviser {

    public static Codec advise(Histogram hist) {
        CompressType type = hist.getType();
        switch (type) {
            case KeyFinger:
                return Codec.INT32;
            case KeyString:
                return Codec.LZ4;
            case KeyHash:
                return adviseForKeyHash(hist);
            case Value:
                return adviseForValue(hist);
            case ValueFast:
                return Codec.Delta;
            default:
                throw new IllegalArgumentException("Unsupported compress type: " + type);
        }
    }

    private static Codec adviseForKeyHash(Histogram hist) {
        // max value determine which numeric type is better
        // including INT8, INT16, INT32.
        int max = (int) hist.getMax();

        // Get bitSet size to represent n values, n equals max
        int bitmapLength = IntegerUtil.numOfBits(max);
        int bitmapBytes = ((bitmapLength - 1) >>> 3) + 1;

        int bytes = IntegerUtil.minBytes(max);
        // byte size
        int byteAlignLength = bytes * hist.getNumOfValues();

        if (max < 256) {
            // TODO: Optimize this condition for better performance with INT16 and INT32
            if (bitmapBytes <= byteAlignLength)
                // If byte size >= bitSet size, bitVector is a better choice
                return Codec.BitVector;
            else
                // max < 2^8
                return Codec.INT8;
        } else if (max < 65536)
            // max < 2^16
            return Codec.INT16;
        else
            // max < 2^32 (Integer.MAX_VALUE)
            return Codec.INT32;
    }

    // TODO: NEED docs
    private static Codec adviseForValue(Histogram hist) {
        if (hist.isSorted())
            return Codec.RLE;

        int max = (int) hist.getMax();
        int bitLength = IntegerUtil.minBits(max);
        int byteLength = IntegerUtil.minBytes(max);
        if (bitLength / (byteLength * 8.0) >= 0.7) {
            switch (byteLength) {
                case 1:
                    return Codec.INT8;
                case 2:
                    return Codec.INT16;
                case 4:
                    return Codec.INT32;
                default:
                    throw new IllegalArgumentException("Unsupported byte length: " + byteLength);
            }
        }
        return Codec.INTBit;
    }

}
