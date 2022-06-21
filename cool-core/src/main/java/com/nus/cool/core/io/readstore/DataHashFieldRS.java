package com.nus.cool.core.io.readstore;

import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.InputVectorFactory;
import com.nus.cool.core.schema.FieldType;

public class DataHashFieldRS implements FieldRS {

    private FieldType fieldType;

    private boolean initialized = false;

    private InputVector valueVector;

    // no used in true logic, to keep compatiable with old version code
    private InputVector keyVector;
    /**
     * BitSet array if this field has been pre-calculated
     */

    @Override
    public FieldType getFieldType() {
        return this.fieldType;
    }

    @Override
    public boolean isSetField() {
        validateInitialization();
        return true;
    }

    /**
     * 
     */
    @Override
    public int getValueByIndex(int idx){
        return this.keyVector.get(this.valueVector.get(idx));
    }

    @Override
    public void readFromWithFieldType(ByteBuffer buf, FieldType ft) {
        this.initialized = true;
        this.fieldType = ft;
        this.keyVector = InputVectorFactory.readFrom(buf);
        this.valueVector = InputVectorFactory.readFrom(buf);
    }

    private void validateInitialization() {
        Preconditions.checkState(this.initialized, "DataHashFieldRS is not initialized");
    }

    @Override
    public void readFrom(ByteBuffer buffer) {
        FieldType fieldType = FieldType.fromInteger(buffer.get());
        this.readFromWithFieldType(buffer, fieldType);
    }

    // under Method is to keep compatiable with old version code

    @Override
    public InputVector getKeyVector() {
        // TODO Auto-generated method stub
        return this.keyVector;
    }

    @Override
    public InputVector getValueVector() {
        // TODO Auto-generated method stub
        return this.valueVector;
    }

    @Override
    public int minKey() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int maxKey() {
        // TODO Auto-generated method stub
        return 0;
    }

}
