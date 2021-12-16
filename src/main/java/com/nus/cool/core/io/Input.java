package com.nus.cool.core.io;

import java.nio.ByteBuffer;

/**
 * Base interface for all read-only data structures
 */
public interface Input {

    /**
     * Read data from byte buffer
     *
     * @param buffer data byte buffer
     */
    void readFrom(ByteBuffer buffer);
}
