package com.nus.cool.core.io.readstore;

import com.nus.cool.core.schema.FieldType;

import java.nio.ByteBuffer;

public class RangeMetaFieldRS implements MetaFieldRS {

    private FieldType fieldType;

    private int min;

    private int max;

    @Override
    public FieldType getFieldType() {
        return this.fieldType;
    }

    @Override
    public int find(String key) {
        return 0;
    }

    @Override
    public int count() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(int i) {
        return null;
    }

    @Override
    public int getMaxValue() {
        return this.max;
    }

    @Override
    public int getMinValue() {
        return this.min;
    }

    @Override
    public void readFromWithFieldType(ByteBuffer buffer, FieldType fieldType) {
        this.fieldType = fieldType;
        this.min = buffer.getInt();
        this.max = buffer.getInt();
    }

    @Override
    public void readFrom(ByteBuffer buffer) {
        FieldType fieldType = FieldType.fromInteger(buffer.get());
        this.readFromWithFieldType(buffer, fieldType);
    }
}
