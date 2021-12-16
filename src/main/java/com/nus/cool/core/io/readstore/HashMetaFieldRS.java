package com.nus.cool.core.io.readstore;

import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.InputVectorFactory;
import com.nus.cool.core.io.storevector.LZ4InputVector;
import com.nus.cool.core.schema.FieldType;
import com.rabinhash.RabinHashFunction32;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static com.google.common.base.Preconditions.checkNotNull;

public class HashMetaFieldRS implements MetaFieldRS {

    private static final RabinHashFunction32 rhash = RabinHashFunction32.DEFAULT_HASH_FUNCTION;

    private Charset charset;

    private FieldType fieldType;

    private InputVector fingerVec;

    private InputVector valueVec;

    public HashMetaFieldRS(Charset charset) {
        this.charset = checkNotNull(charset);
    }

    @Override
    public FieldType getFieldType() {
        return this.fieldType;
    }

    @Override
    public int find(String key) {
        return this.fingerVec.find(rhash.hash(key));
    }

    @Override
    public int count() {
        return this.fingerVec.size();
    }

    @Override
    public String getString(int i) {
        return ((LZ4InputVector) this.valueVec).getString(i, this.charset);
    }

    @Override
    public int getMaxValue() {
        return this.count() - 1;
    }

    @Override
    public int getMinValue() {
        return 0;
    }

    @Override
    public void readFromWithFieldType(ByteBuffer buffer, FieldType fieldType) {
        this.fieldType = fieldType;
        this.fingerVec = InputVectorFactory.readFrom(buffer);
        if (this.fieldType == FieldType.Action || this.fieldType == FieldType.Segment || this.fieldType == FieldType.UserKey)
            this.valueVec = InputVectorFactory.readFrom(buffer);
    }

    @Override
    public void readFrom(ByteBuffer buffer) {
        FieldType fieldType = FieldType.fromInteger(buffer.get());
        this.readFromWithFieldType(buffer, fieldType);
    }
}
