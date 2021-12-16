package com.nus.cool.core.io.storevector;

import com.nus.cool.core.io.Input;

/**
 * An ordered collection(sequence) of integers. Implementation of this interface
 * should at least implements sequential access method(i.e., hasNext() and next()).
 * <p>
 * If random access method(i.e., find() and get()) is implemented. The find() should
 * be completed at O(log(n)) and the get() should be completed at O(1).
 */
public interface InputVector extends Input {

    /**
     * Get number of values of this vector
     *
     * @return number of values
     */
    int size();

    /**
     * Find index by key
     *
     * @param key target value
     * @return index
     */
    int find(int key);

    /**
     * Get value by index
     *
     * @param index index
     * @return target value
     */
    int get(int index);

    /**
     * Get vector has next or not
     *
     * @return boolean value has next
     */
    boolean hasNext();

    /**
     * Get next value in vector
     *
     * @return next value
     */
    int next();

    /**
     * Skip to specific position
     *
     * @param pos target position
     */
    void skipTo(int pos);
}
