package com.nus.cool.core.io.readstore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;

import com.nus.cool.core.io.compression.SimpleBitSetCompressor;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.InputVectorFactory;
import com.nus.cool.core.schema.Codec;
import com.nus.cool.core.schema.FieldType;

public class DataHashFieldRS implements DataFieldRS{


    private FieldType fieldType;

    private final Codec type = Codec.Set;

    private ArrayList<Integer> valueVector;

    /**
   * BitSet array if this field has been pre-calculated
   */
    
    @Override
    public FieldType getFieldType() {
        return this.fieldType;
    }

    @Override
    public ArrayList<Integer> getValueVector() {
        return null;
    }

    @Override
    public boolean isSetField() {
        return true;
    }

    @Override
    public void readFromBuffer(ByteBuffer buf, FieldType ft) {
        this.fieldType = ft;
        InputVector local2Global = InputVectorFactory.readFrom(buf);
        InputVector localInputvector = InputVectorFactory.readFrom(buf);
        this.valueVector = new ArrayList<>();
        
        // Transfer LocalIdList to GloablIdList.
        while(localInputvector.hasNext()){
            this.valueVector.add(local2Global.get(localInputvector.next()));
        }
    }

 
    
}
