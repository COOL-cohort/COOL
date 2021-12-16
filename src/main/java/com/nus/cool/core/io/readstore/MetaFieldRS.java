package com.nus.cool.core.io.readstore;

import com.nus.cool.core.io.Input;
import com.nus.cool.core.schema.FieldType;

import java.nio.ByteBuffer;

public interface MetaFieldRS extends Input {

    FieldType getFieldType();

    int find(String key);

    int count();

    String getString(int i);

    int getMaxValue();

    int getMinValue();

    void readFromWithFieldType(ByteBuffer buffer, FieldType fieldType);
}
