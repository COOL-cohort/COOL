package com.nus.cool.core.io.writestore;

import java.io.DataOutput;
import java.io.IOException;

import com.nus.cool.core.schema.FieldType;

public class DataInvariantFieldWS implements DataFieldWS{ 
    
    private FieldType fieldType;

    public DataInvariantFieldWS(FieldType fieldType){
        this.fieldType = fieldType;
    }

    @Override
    public int writeTo(DataOutput out) throws IOException {
        return 0;
    }

    @Override
    public FieldType getFieldType() {
        return this.fieldType;
    }

    @Override
    public void put(String tuple) throws IOException {
        // for invariant data field, no need to write data
    }
    
}
