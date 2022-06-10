package com.nus.cool.core.io.readstore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;

import com.nus.cool.core.io.compression.SimpleBitSetCompressor;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.InputVectorFactory;
import com.nus.cool.core.schema.Codec;
import com.nus.cool.core.schema.FieldType;

public class DataRangeFieldRS implements DataFieldRS {

    private FieldType fieldType;
    
    private final Codec type = Codec.Range;

    private int minKey;
    private int maxKey;

    private BitSet[] bitSets;
    
    private ArrayList<Integer> valueVector;


    @Override
    public FieldType getFieldType() {
        return this.fieldType;
    }

    @Override
    public ArrayList<Integer> getValueVector() {
        return this.valueVector;
    }

    public int minKey() {
        return this.minKey;
    }

    public int maxKey() {
        return this.maxKey;
    }

    @Override
    public boolean isSetField() {
        return type == Codec.Set;
    }

    @Override
    public void readFromBuffer(ByteBuffer buf, FieldType ft) {
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
    
}
