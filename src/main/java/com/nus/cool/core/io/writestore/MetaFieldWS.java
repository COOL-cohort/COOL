package com.nus.cool.core.io.writestore;

import com.nus.cool.core.io.Output;
import com.nus.cool.core.schema.FieldType;

public interface MetaFieldWS extends Output {

    /**
     * Put value into this field
     *
     * @param v value
     */
    void put(String v);

    /**
     * Find the index of value in this meta field,
     * return -1 if no such value exists
     *
     * @param v target value
     * @return index of value in this meta field
     */
    int find(String v);

    /**
     * Number of entries in this field
     *
     * @return number of entries in this field
     */
    int count();

    /**
     * Get field type of this field
     *
     * @return fieldType of this field
     */
    FieldType getFieldType();

    /**
     * Call this method before writeTo when no more values
     * are put into this meta field. After the method returns,
     * this meta field is frozen for writing.
     */
    void complete();
}
