package com.nus.cool.core.io.readstore;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.google.common.base.Preconditions;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.InputVectorFactory;
import com.nus.cool.core.schema.FieldType;

public class DataHashFieldRS implements DataFieldRS{


    private FieldType fieldType;

    private ArrayList<Integer> valueVector;

    private boolean initialized = false;
    /**
   * BitSet array if this field has been pre-calculated
   */
    
    @Override
    public FieldType getFieldType() {
        return this.fieldType;
    }

    @Override
    public ArrayList<Integer> getValueVector() {
        validateInitialization();
        return this.valueVector;
    }

    @Override
    public boolean isSetField() {
        validateInitialization();
        return true;
    }
    
    @Override
    public int getTupleNumber(){
        validateInitialization();
        return this.valueVector.size();
    }

    @Override
    public void readFromBuffer(ByteBuffer buf, FieldType ft) {
        this.initialized = true;
        this.fieldType = ft;
        InputVector local2Global = InputVectorFactory.readFrom(buf);
        InputVector localInputvector = InputVectorFactory.readFrom(buf);
        this.valueVector = new ArrayList<>();
        
        // Transfer LocalIdList to GloablIdList.
        while(localInputvector.hasNext()){
            this.valueVector.add(local2Global.get(localInputvector.next()));
        }
    }

    
    private void validateInitialization(){
        Preconditions.checkState(this.initialized, "DataHashFieldRS is not initialized" );
    }
    
}
