package com.nus.cool.core.io.storevector;

import com.nus.cool.core.schema.Codec;

import java.nio.ByteBuffer;

/**
 * Decompress stored data
 */
public class InputVectorFactory {

    public static InputVector readFrom(ByteBuffer buffer) {
        Codec codec = Codec.fromInteger(buffer.get());
        InputVector result = null;
        switch (codec) {
            case INT8:
                return (InputVector) ZInt8Store.load(buffer, buffer.getInt());
            case INT16:
                return (InputVector) ZInt16Store.load(buffer, buffer.getInt());
            case INT32:
                return (InputVector) ZInt32Store.load(buffer, buffer.getInt());
            case BitVector:
                result = new BitVectorInputVector();
                result.readFrom(buffer);
                return result;
            case LZ4:
                result = new LZ4InputVector();
                result.readFrom(buffer);
                return result;
            case RLE:
                result = new RLEInputVector();
                result.readFrom(buffer);
                return result;
            case INTBit:
                return ZIntBitInputVector.load(buffer);
            case Delta:
                result = new FoRInputVector();
                result.readFrom(buffer);
                return result;
            default:
                throw new IllegalArgumentException("Unsupported codec: " + codec);
        }
    }
}
