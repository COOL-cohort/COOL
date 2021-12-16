package com.nus.cool.core.io.readstore;

import com.nus.cool.core.io.compression.SimpleBitSetCompressor;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.InputVectorFactory;
import com.nus.cool.core.schema.Codec;
import com.nus.cool.core.schema.FieldType;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * Cool field read store, both hash field and range field
 * <p>
 * hash field Layout
 * -----------------
 * | keys | values |
 * -----------------
 * where
 * keys = globalIDs (compressed)
 * values = column data, stored as localIDs (compressed)
 * <p>
 * range field layout
 * ------------------------------
 * | codec | min | max | values |
 * ------------------------------
 * where
 * min = min of the values
 * max = max of the values
 * values = column data (compressed)
 */
public class CoolFieldRS implements FieldRS {

    private FieldType fieldType;

    private boolean bRangeField;

    private boolean bSetField;

    private int minKey;

    private int maxKey;

    /**
     * key vector for hash field
     */
    private InputVector keyVec;

    /**
     * value vector for hash field
     */
    private InputVector valueVec;

    /**
     * BitSet array if this field has been pre-calculated
     */
    private BitSet[] bitSets;

    @Override
    public void readFrom(ByteBuffer buffer) {
        // Get field type
        this.fieldType = FieldType.fromInteger(buffer.get());
        Codec codec = Codec.fromInteger(buffer.get());
        if (codec == Codec.Range) {
            // Range field case
            this.minKey = buffer.getInt();
            this.maxKey = buffer.getInt();
            this.bRangeField = true;
        } else {
            // Hash field case
            buffer.position(buffer.position() - 1);
            this.keyVec = InputVectorFactory.readFrom(buffer);
            this.minKey = 0;
            this.maxKey = this.keyVec.size();
            this.bSetField = true;
        }

        codec = Codec.fromInteger(buffer.get());
        if (codec == Codec.PreCAL) {
            int values = buffer.get();
            this.bitSets = new BitSet[values];
            for (int i = 0; i < values; i++)
                this.bitSets[i] = SimpleBitSetCompressor.read(buffer);
        } else {
            buffer.position(buffer.position() - 1);
            this.valueVec = InputVectorFactory.readFrom(buffer);
        }
    }

    @Override
    public InputVector getKeyVector() {
        return this.keyVec;
    }

    @Override
    public InputVector getValueVector() {
        return this.valueVec;
    }

    @Override
    public int minKey() {
        return this.minKey;
    }

    @Override
    public int maxKey() {
        return this.maxKey;
    }

    @Override
    public boolean isSetField() {
        return this.bSetField;
    }

    @Override
    public void readFromWithFieldType(ByteBuffer buffer, FieldType fieldType) {
        this.fieldType = fieldType;
        Codec codec = Codec.fromInteger(buffer.get());
        if (codec == Codec.Range) {
            // Range field case
            this.minKey = buffer.getInt();
            this.maxKey = buffer.getInt();
            this.bRangeField = true;
        } else {
            // Hash field case
            buffer.position(buffer.position() - 1);
            this.keyVec = InputVectorFactory.readFrom(buffer);
            this.minKey = 0;
            this.maxKey = this.keyVec.size();
            this.bSetField = true;
        }

        codec = Codec.fromInteger(buffer.get());
        if (codec == Codec.PreCAL) {
            int values = buffer.get();
            this.bitSets = new BitSet[values];
            for (int i = 0; i < values; i++)
                this.bitSets[i] = SimpleBitSetCompressor.read(buffer);
        } else {
            buffer.position(buffer.position() - 1);
            this.valueVec = InputVectorFactory.readFrom(buffer);
        }
    }
}
