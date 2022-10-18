package com.nus.cool.core.io.readstore;

import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;


import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.InputVectorFactory;
import com.nus.cool.core.io.storevector.LZ4InputVector;
import com.nus.cool.core.schema.FieldType;
import com.rabinhash.RabinHashFunction32;

public class MetaUserFieldRS implements MetaFieldRS {

    protected static final RabinHashFunction32 rhash = RabinHashFunction32.DEFAULT_HASH_FUNCTION;

    protected Charset charset;

    protected MetaChunkRS metaChunkRS;
    
    protected FieldType fieldType;


    protected InputVector fingerVec;

    protected InputVector globalIDVec;

    protected InputVector valueVec;

    protected InputVector[] invarantMaps; // the idx is the globalIdIdx, the value is the globalId of invariant field
    
    public MetaUserFieldRS(MetaChunkRS metaChunkRS, Charset charset){
        this.charset = checkNotNull(charset);
        this.metaChunkRS = metaChunkRS;
        int invariant_size = metaChunkRS.getSchema().getInvariantFieldNumber();
        this.invarantMaps = new InputVector[invariant_size];
    }

    @Override
    public void readFrom(ByteBuffer buffer) {
        FieldType fieldType = FieldType.fromInteger(buffer.get());
        this.readFromWithFieldType(buffer, fieldType);
    }

    @Override
    public FieldType getFieldType() {
        return this.fieldType;
    }

    @Override
    public int find(String key) {
        int globalIdIdx = this.fingerVec.find(rhash.hash(key));
        return this.globalIDVec.get(globalIdIdx);
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
        this.globalIDVec = InputVectorFactory.readFrom(buffer);
        for(int i = 0; i < this.invarantMaps.length; i++){
            this.invarantMaps[i] = InputVectorFactory.readFrom(buffer);
        }
        this.valueVec = InputVectorFactory.readFrom(buffer);
    }

    // --------------- specific method for MetaUserField ---------------
    /**
     * 
     * @param invariant_idx, the index of invariant field in all invariant fields
     * @param gid, the according gloablId of UserKey
     * @return
     */
    public int getInvaraintValue(int invariant_idx, int gid){
        return this.invarantMaps[invariant_idx].get(gid);
    }
    
}
