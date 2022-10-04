package com.nus.cool.core.io.readstore;

import java.nio.ByteBuffer;

import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.FieldType;

public class DataInvariantRangeFieldRS implements FieldRS{

    private final MetaUserFieldRS userMetaField;

    private final DataHashFieldRS userDataField;

    private final FieldType fieldType;

    private final int invariant_idx;

    public DataInvariantRangeFieldRS(FieldType fieldType, int invariant_idx,MetaUserFieldRS userMetaField, DataHashFieldRS  userDataField){
        this.userMetaField = userMetaField;
        this.userDataField = userDataField;
        this.fieldType = fieldType;
        this.invariant_idx = invariant_idx;
    }
    

    @Override
    public FieldType getFieldType() {
        return this.fieldType;
    }

    @Override
    public int getValueByIndex(int idx) {
        int gidOfUserKey = this.userDataField.getValueByIndex(idx);
        return this.userMetaField.getInvaraintValue(this.invariant_idx, gidOfUserKey);
    }

    // -------------- above method is no used in new Version -----------------
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


    @Override
    public void readFrom(ByteBuffer buffer) {
        // TODO Auto-generated method stub
        
    }


    
}
