package com.nus.cool.core.io.readstore;

import com.google.common.collect.Maps;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.InputVectorFactory;
import com.nus.cool.core.schema.FieldType;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.Set;

public class UserMetaFieldRS extends MetaHashFieldRS{
    private Map<String, Integer> invariantName2Id = Maps.newHashMap();
    private List<InputVector> userToInvariant;

    public UserMetaFieldRS(Charset charset, Map<String, Integer> invariantName2Id) {
        super(charset);
        this.invariantName2Id=invariantName2Id;
    }

    public int find(int hash) {
        return this.fingerVec.find(hash);
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

    @Override
    public int find(String string){
        int hashkey = rhash.hash(string);
        int globalIDIdx = this.fingerVec.find(hashkey);
        return this.globalIDVec.get(globalIDIdx);
    }
}
