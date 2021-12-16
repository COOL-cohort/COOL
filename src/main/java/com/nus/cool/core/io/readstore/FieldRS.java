package com.nus.cool.core.io.readstore;

import com.nus.cool.core.io.Input;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.FieldType;

import java.nio.ByteBuffer;

public interface FieldRS extends Input {

    InputVector getKeyVector();

    InputVector getValueVector();

    int minKey();

    int maxKey();

    boolean isSetField();

    void readFromWithFieldType(ByteBuffer buf, FieldType fieldType);
}
