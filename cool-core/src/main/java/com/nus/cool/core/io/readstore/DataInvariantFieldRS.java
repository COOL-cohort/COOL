package com.nus.cool.core.io.readstore;

import java.nio.ByteBuffer;

import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.FieldType;

public class DataInvariantFieldRS implements FieldRS {

    private final MetaUserFieldRS userMetaField;

    private final ChunkRS dataChunkRS;

    DataInvariantFieldRS(MetaUserFieldRS userMetaField, ChunkRS dataChunkRS){
        this.userMetaField = userMetaField;
        this.dataChunkRS = dataChunkRS;
    }

    @Override
    public FieldType getFieldType() {
        // TODO Auto-generated method stub
        return null;
    }

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
    public int getValueByIndex(int idx) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void readFrom(ByteBuffer buffer) { }
    
}
