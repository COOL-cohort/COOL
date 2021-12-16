package com.nus.cool.core.io.storevector;

import com.nus.cool.core.schema.Codec;
import com.sun.org.apache.bcel.internal.classfile.Code;

import java.nio.ByteBuffer;

public class FoRInputVector implements InputVector {

    private int min;

    private int max;

    private InputVector vecIn;

    @Override
    public int size() {
        return this.vecIn.size();
    }

    @Override
    public int find(int key) {
        if (key < this.min || key > this.max)
            return -1;
        return this.vecIn.find(key - this.min);
    }

    @Override
    public int get(int index) {
        return this.min + this.vecIn.get(index);
    }

    @Override
    public boolean hasNext() {
        return this.vecIn.hasNext();
    }

    @Override
    public int next() {
        return this.min + this.vecIn.next();
    }

    @Override
    public void skipTo(int pos) {
        this.vecIn.skipTo(pos);
    }

    @Override
    public void readFrom(ByteBuffer buffer) {
        this.min = buffer.getInt();
        this.max = buffer.getInt();
        Codec codec = Codec.fromInteger(buffer.get());
        switch (codec) {
            case INT8:
                this.vecIn = (InputVector) ZInt8Store.load(buffer, buffer.getInt());
                break;
            case INT16:
                this.vecIn = (InputVector) ZInt16Store.load(buffer, buffer.getInt());
                break;
            case INT32:
                this.vecIn = (InputVector) ZInt32Store.load(buffer, buffer.getInt());
                break;
            default:
                throw new IllegalArgumentException("Unsupported codec: " + codec);
        }
    }
}
