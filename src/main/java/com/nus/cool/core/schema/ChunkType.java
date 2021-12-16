package com.nus.cool.core.schema;

/**
 * Chunk type
 */
public enum ChunkType {
    /**
     * Chunk type for MetaChunk
     */
    META;

    public static ChunkType fromInteger(int i) {
        switch (i) {
            case 0:
                return META;
            default:
                throw new IllegalArgumentException("Invalid chunk type int: " + i);
        }
    }
}
