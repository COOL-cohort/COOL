package com.nus.cool.core.util.reader;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface to read tuple from source
 */
public interface TupleReader extends Closeable {

    /**
     * Check if source has next tuple
     *
     * @return boolean value
     */
    boolean hasNext();

    /**
     * Reads tuple
     *
     * @return tuple
     * @throws IOException If an I/O error occurs
     */
    Object next() throws IOException;
}
