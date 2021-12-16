package com.nus.cool.core.io.writestore;

import com.nus.cool.core.io.Output;
import com.nus.cool.core.schema.FieldType;

import java.io.IOException;

public interface FieldWS extends Output {

    /**
     * Get field type of this field
     *
     * @return fieldType of this field
     */
    FieldType getFieldType();

    /**
     * Put tuple into this field
     *
     * @param tuple value
     */
    void put(String[] tuple) throws IOException;
}
