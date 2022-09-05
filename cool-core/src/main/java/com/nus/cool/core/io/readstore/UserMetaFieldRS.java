package com.nus.cool.core.io.readstore;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Maps;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.InputVectorFactory;
import com.nus.cool.core.io.storevector.LZ4InputVector;
import com.nus.cool.core.schema.FieldType;
import com.rabinhash.RabinHashFunction32;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

import lombok.Getter;

public class UserMetaFieldRS implements MetaFieldRS{
    private static final RabinHashFunction32 rhash = RabinHashFunction32.DEFAULT_HASH_FUNCTION;

    private Charset charset;
    private Map<String, Integer> invariantName2Id = Maps.newHashMap();

    private FieldType fieldType;

    private InputVector fingerVec;
    private InputVector globalIDVec;
    private List<InputVector> userToInvariant;
    private InputVector valueVec;

    // inverse map from global id to the offset in values.
    //  only populated once when getString is called to retrieve from valueVec
    private Map<Integer, Integer> id2offset;

    public UserMetaFieldRS(Charset charset, Map<String, Integer> invariantName2Id) {
        this.charset = checkNotNull(charset);
        this.invariantName2Id=invariantName2Id;
    }

    @Override
    public FieldType getFieldType() {
        return this.fieldType;
    }

    @Override
    public int find(String key) {
        int globalIDIdx = this.fingerVec.find(rhash.hash(key));
        return this.globalIDVec.get(globalIDIdx);
    }

    public int find(int hash) {
        return this.fingerVec.find(hash);
    }

    @Override
    public int count() {
        return this.fingerVec.size();
    }

    @Override
    public String getString(int i) {
        if (this.id2offset == null) {
            this.id2offset = Maps.newHashMap();
            // lazily populate the inverse index only once
            for (int j = 0; j < this.globalIDVec.size(); j++) {
                this.id2offset.put(this.globalIDVec.get(j), j);
            }
        }
        return ((LZ4InputVector) this.valueVec)
                .getString(this.id2offset.get(i), this.charset);
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
        this.userToInvariant=new ArrayList<>(this.invariantName2Id.size());
        for(int i=0;i<this.invariantName2Id.size();i++)
        {
            this.userToInvariant.add(i,InputVectorFactory.readFrom(buffer));
        }
        this.valueVec = InputVectorFactory.readFrom(buffer);
    }

    @Override
    public void readFrom(ByteBuffer buffer) {
        FieldType fieldType = FieldType.fromInteger(buffer.get());
        this.readFromWithFieldType(buffer, fieldType);
    }
    public String[] getGidMap(){
        // Can store it and reuse ret (suggestion)
        String[] ret = new String[this.count()];
        LZ4InputVector strlist = (LZ4InputVector) this.valueVec;
        for(int i = 0; i < ret.length; i++){
            ret[this.globalIDVec.get(i)] = strlist.getString(i, this.charset);
        }
        return ret;
    }
    public int[] getInvariantGidMap(String invariantFieldName){
        int[] ret = new int[this.count()];
        int invariantFiledIndex=this.invariantName2Id.get(invariantFieldName)-1;
        InputVector invariantVector = this.userToInvariant.get(invariantFiledIndex);
        for(int i = 0; i < ret.length; i++){
            ret[this.globalIDVec.get(i)] = invariantVector.get(i);
        }
        return ret;

    }

    public Set<String> getInvariantName(){
        return this.invariantName2Id.keySet();
    }
}
