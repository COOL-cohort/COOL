package com.nus.cool.core.io.compression;

import com.google.common.primitives.Longs;
import com.nus.cool.core.util.IntegerUtil;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

// TODO: NEED docs
public class ZIntBitCompressor implements Compressor {

    private int numOfBits;

    private int maxCompressedLength;

    public ZIntBitCompressor(Histogram hist) {
        if (hist.getMax() >= (1L << 32))
            numOfBits = 64;
        else
            this.numOfBits = IntegerUtil.minBits((int) (hist.getMax() + 1));
        int numOfVal = hist.getNumOfValues();

        int numOfValPerPack = 64 / numOfBits;
        int numOfPack = (numOfVal - 1) / numOfValPerPack + 2;
        this.maxCompressedLength = numOfPack * Longs.BYTES;
    }

    @Override
    public int maxCompressedLength() {
        return this.maxCompressedLength;
    }

    @Override
    public int compress(byte[] src, int srcOff, int srcLen, byte[] dest,
                        int destOff, int maxDestLen) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compress(int[] src, int srcOff, int srcLen, byte[] dest,
                        int destOff, int maxDestLen) {

        ByteBuffer buffer = ByteBuffer.wrap(dest, destOff, maxDestLen);
        buffer.order(ByteOrder.nativeOrder());
        buffer.putInt(srcLen);
        buffer.putInt(numOfBits);

        Pack packer = new Pack();
        for (int i = 0; i < srcLen; i++) {
            packer.pushNext(src[i + srcOff]);

            if (!packer.hasSlot()) {
                buffer.putLong(packer.pack);
                packer.reset();
            }
        }

        if (packer.offset < 64) {
            buffer.putLong(packer.pack);
        }

        //System.out.println(packer.offset);

        return buffer.position();
    }

    class Pack {
        int offset;
        long pack;

        Pack() {
            reset();
        }

        boolean hasSlot() {
            return offset >= numOfBits;
        }

        void pushNext(long val) {
            offset -= numOfBits;
            pack |= (val << offset);
        }

        void reset() {
            offset = 64;
            pack = 0;
        }
    }
}
