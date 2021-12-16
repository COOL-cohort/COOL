package com.nus.cool.core.io.compression;

import com.nus.cool.core.schema.CompressType;
import lombok.Builder;
import lombok.Getter;

/**
 * Properties for compress
 */
@Getter
@Builder
public class Histogram {

    /**
     * Compress data size
     */
    private int rawSize;

    /**
     * Number of values if compress data is countable
     */
    private int numOfValues;

    /**
     * Max(Last) value in compress data
     */
    private long max;

    /**
     * Min(First) value in compress data
     */
    private long min;

    /**
     * Specific compress type
     */
    private CompressType type;
}
