package com.nus.cool.core.io;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Base interface for all write-only data structures
 */
public interface Output {

    /**
     * Write data to output stream and return bytes written
     *
     * @param out stream can write data to output stream
     * @return bytes written
     * @throws IOException If an I/O error occurs
     */
    int writeTo(DataOutput out) throws IOException;
}
