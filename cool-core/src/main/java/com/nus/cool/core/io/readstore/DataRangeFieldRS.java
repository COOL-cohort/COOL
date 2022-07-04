package com.nus.cool.core.io.readstore;

import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.InputVectorFactory;
import com.nus.cool.core.schema.FieldType;

public class DataRangeFieldRS implements FieldRS {

    private FieldType fieldType;
    

    private int minKey;
    private int maxKey;

    private boolean initialized = false;
    private InputVector valueVector;


    @Override
    public FieldType getFieldType() {
        return this.fieldType;
    }


    @Override
    public int minKey() {
        validateInitialization();
        return this.minKey;
    }

    @Override
    public int maxKey() {
        validateInitialization();
        return this.maxKey;
    }

    @Override
    public boolean isSetField() {
        validateInitialization();
        return false;
    }

    @Override
    public int getValueByIndex(int idx){
        return this.valueVector.get(idx);
    }

    @Override
    public void readFromWithFieldType(ByteBuffer buf, FieldType ft) {
        this.initialized = true;
        this.fieldType = ft;
        this.valueVector = InputVectorFactory.readFrom(buf);
        // TODO(Lingze) There is still room for optimization
        // We can directly read from buffer to ArrayList<Integar>

    }

    private void validateInitialization(){
        Preconditions.checkState(this.initialized, "DataRangeFiledRS is not initialized");
    }   


    @Override
    public void readFrom(ByteBuffer buffer) {
        FieldType fieldType = FieldType.fromInteger(buffer.get());
        this.readFromWithFieldType(buffer, fieldType);
    }
    

    //no used, only to keep compatiable with old version code
    @Override
    public InputVector getKeyVector() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public InputVector getValueVector() {
        // TODO Auto-generated method stub
        return null;
    }

}
