package com.nus.cool.core.io.readstore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;

import com.google.common.base.Preconditions;
import com.nus.cool.core.io.compression.SimpleBitSetCompressor;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.InputVectorFactory;
import com.nus.cool.core.schema.Codec;
import com.nus.cool.core.schema.FieldType;

public class DataRangeFieldRS implements DataFieldRS {

    private FieldType fieldType;
    

    private int minKey;
    private int maxKey;

    private boolean initialized = false;
    
    private ArrayList<Integer> valueVector;


    @Override
    public FieldType getFieldType() {
        return this.fieldType;
    }

    @Override
    public ArrayList<Integer> getValueVector() {
        validateInitialization();
        return this.valueVector;
    }

    public int minKey() {
        validateInitialization();
        return this.minKey;
    }

    public int maxKey() {
        validateInitialization();
        return this.maxKey;
    }

    @Override
    public int getTupleNumber(){
        validateInitialization();
        return this.valueVector.size();
    }
    @Override
    public boolean isSetField() {
        validateInitialization();
        return false;
    }

    @Override
    public void readFromBuffer(ByteBuffer buf, FieldType ft) {
        this.initialized = true;
        this.fieldType = ft;
        this.minKey = buf.getInt();
        this.maxKey = buf.getInt();
        // Note : No calculate PreCal
        InputVector list = InputVectorFactory.readFrom(buf);
        // TODO(Lingze) There is still room for optimization
        // We can directly read from buffer to ArrayList<Integar>
        this.valueVector = new ArrayList<>(list.size());
        while(list.hasNext()){
            this.valueVector.add(list.next());
        }
    }

    private void validateInitialization(){
        Preconditions.checkState(this.initialized, "DataRangeFiledRS is not initialized");
    }
    
}
