package com.nus.cool.core.io.readstore;

import java.nio.ByteBuffer;

import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.Codec;
import com.nus.cool.core.schema.FieldType;

public interface DataFieldRS {

    public FieldType getFieldType();
    
    public ArrayList<Integer> getValueVector();

    public boolean isSetField();

    public void readFromBuffer(ByteBuffer buf, FieldType ft);

    public static DataFieldRS ReadFieldRS(ByteBuffer buf){
        FieldType fieldtype = FieldType.fromInteger(buf.get());
        Codec codec = Codec.fromInteger(buf.get());
        if(codec == Codec.Range) {
            // Generate DataRangeFieldRS
            DataRangeFieldRS rs = new DataRangeFieldRS();
            rs.readFromBuffer(buf,fieldtype);
            return rs;
        } else {
            // Generate DataHashFieldRS
            DataHashFieldRS rs = new DataHashFieldRS();
            rs.readFromBuffer(buf,fieldtype);
            return rs;
        }
    }
}
