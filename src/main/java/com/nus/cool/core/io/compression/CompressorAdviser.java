package com.nus.cool.core.io.compression;

import com.nus.cool.core.schema.Codec;
import com.nus.cool.core.schema.CompressType;

/**
 * Advise codec according to compress type
 * <p>
 * KeyFinger -> LZ4
 * KeyString -> INT32
 */
public class CompressorAdviser {

    public static Codec advise(Histogram hist) {
        CompressType type = hist.getType();
        switch (type) {
            case KeyFinger:
                return Codec.INT32;
            case KeyString:
                return Codec.LZ4;
            default:
                throw new IllegalArgumentException("Unsupported compress type: " + type);
        }
    }

}
