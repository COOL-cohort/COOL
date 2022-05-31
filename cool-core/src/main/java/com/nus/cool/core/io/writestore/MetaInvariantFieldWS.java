package com.nus.cool.core.io.writestore;

import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import com.nus.cool.core.schema.DataType;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.util.IntegerUtil;
import lombok.Getter;

import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetaInvariantFieldWS implements MetaFieldWS {

//    private final FieldType[] dataType;

    @Getter
    private Map<String, List<Object>> metaInvariantFields = new HashMap<>();

    @Getter
    private List<Integer> invariantIndex;

    @Getter
    private final Integer userKeyIndex;


    public MetaInvariantFieldWS(List<Integer> invariantIndex, Integer userKeyIndex) {

//        this.dataType = dataType;
        this.invariantIndex = invariantIndex;
        this.userKeyIndex = userKeyIndex;
    }

    @Override
    public int writeTo(DataOutput out) throws IOException {
        int bytesWritten = 0;
        for(String key:metaInvariantFields.keySet()){

            out.writeChars(key);
            bytesWritten += key.getBytes().length;
            List<Object> tempList=metaInvariantFields.get(key);
            for(int i=0;i<tempList.size();i++)
            {
                if(tempList.get(i).getClass().toString()=="int"){
                    out.writeInt(IntegerUtil.toNativeByteOrder((int)tempList.get(i)));
                    bytesWritten += Ints.BYTES;
                }
                else{
                    out.writeChars(tempList.get(i).toString());
                }
            }
        }
        return bytesWritten;
    }

    public void putInvariant(String UserID, List<Object> invariantData) {
        this.metaInvariantFields.put(UserID, invariantData);
    }

    @Override
    public void put(String v) {

    }

    @Override
    public void update(String v) {

    }

    @Override
    public int find(String v) {
        return 0;
    }

    @Override
    public int count() {
        return 0;
    }

    @Override
    public FieldType getFieldType() {
        return null;
    }

    @Override
    public void complete() {

    }
}
