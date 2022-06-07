package com.nus.cool.core.io.writestore;

import com.google.common.primitives.Ints;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.util.IntegerUtil;
import lombok.Getter;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
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

    @Getter
    private List<Integer> invariantTypes = new ArrayList<>();

    public MetaInvariantFieldWS(List<Integer> invariantIndex, Integer userKeyIndex) {

//        this.dataType = dataType;
        this.invariantIndex = invariantIndex;
        this.userKeyIndex = userKeyIndex;
    }

    @Override
    public int writeTo(DataOutput out) throws IOException {
        int bytesWritten = 0;
        for (int i = 0; i < invariantTypes.size(); i++) {
            out.writeInt(invariantTypes.get(i));
            bytesWritten += Ints.BYTES;
        }
        for (String key : metaInvariantFields.keySet()) {
            out.writeInt(key.getBytes().length);
            bytesWritten+=Ints.BYTES;
            out.writeChars(key);
            bytesWritten += key.getBytes().length;
            List<Object> tempList = metaInvariantFields.get(key);
            for (int i = 0; i < tempList.size(); i++) {
                if (tempList.get(i).getClass().toString() == "int") {
//                    out.writeInt(Ints.BYTES);
//                    bytesWritten += Ints.BYTES;
                    out.writeInt(IntegerUtil.toNativeByteOrder((int) tempList.get(i)));
                    bytesWritten += Ints.BYTES;
                } else {
                    out.writeInt(tempList.get(i).toString().getBytes().length);
                    out.writeChars(tempList.get(i).toString());
                    bytesWritten += tempList.get(i).toString().getBytes().length;
                    bytesWritten += Ints.BYTES;
                }
            }
        }
        return bytesWritten;
    }

    public void putInvariant(String UserID, List<Object> invariantData) {
        if (this.metaInvariantFields.size() == 0) {
            for (int i = 0; i < invariantData.size(); i++) {
                if (invariantData.get(i).getClass().toString() == "int") {
                    this.invariantTypes.add(0);
                } else {
                    this.invariantTypes.add(1);
                }
            }
        }
        if (this.metaInvariantFields.containsKey(UserID)) {
            return;
        } else {
            this.metaInvariantFields.put(UserID, invariantData);
        }
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
