package com.nus.cool.core.schema;

/**
 * Chunk type
 */
public enum ChunkType {
    /**
     * Chunk type for MetaChunk
     */
    META,

    /**
     * Chunk type for DataChunk
     */
    DATA;

    public static ChunkType fromInteger(int c) {
        switch (c) {
            case 0:
                return META;
            case 1:
                return DATA;
            default:
                throw new IllegalArgumentException("Unsupported chunk type ordinal: " + c);
        }
    }

}
