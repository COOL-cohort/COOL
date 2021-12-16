package com.nus.cool.core.schema;

/**
 * Code for decompressor
 */
public enum Codec {

    /**
     * Code for int8 data
     */
    INT8,

    /**
     * Code for int16 data
     */
    INT16,

    /**
     * Code for int32 data
     */
    INT32,

    /**
     * Code for bit vector
     */
    BitVector,

    /**
     * Code for string values data
     */
    LZ4,

    /**
     * Code for pre calculate data
     */
    PreCAL,

    /**
     * Code for sorted data
     */
    RLE,

    // TODO: NEED docs
    INTBit,

    /**
     * Code for range, write a min value and a max value directly.
     */
    Range,

    /**
     * Code for numeric data, use delta encoding
     */
    Delta

}
