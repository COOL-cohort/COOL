package com.nus.cool.core.io.storevector;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static com.google.common.base.Preconditions.checkArgument;

public class LZ4InputVector implements InputVector {

    private int zLen;

    private int rawLen;

    private ByteBuffer buffer;

    private int[] offsets;

    private byte[] data;

    private LZ4FastDecompressor decompressor = LZ4Factory.fastestInstance().fastDecompressor();

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int find(int key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int get(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasNext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int next() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skipTo(int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFrom(ByteBuffer buffer) {
        this.zLen = buffer.getInt();
        this.rawLen = buffer.getInt();
        int oldLimit = buffer.limit();
        int newLimit = buffer.position() + this.zLen;
        buffer.limit(newLimit);
        this.buffer = buffer.slice().order(buffer.order());
        buffer.position(newLimit);
        buffer.limit(oldLimit);
    }

    public String getString(int index, Charset charset) {
        if (this.buffer.hasRemaining()) {
            byte[] compressed = new byte[this.zLen];
            byte[] raw = new byte[this.rawLen];
            this.buffer.get(compressed);
            this.decompressor.decompress(compressed, raw, this.rawLen);
            ByteBuffer buffer = ByteBuffer.wrap(raw);
            int values = buffer.getInt();
            this.offsets = new int[values];
            for (int i = 0; i < values; i++)
                this.offsets[i] = buffer.getInt();
            this.data = new byte[rawLen - 4 - values * 4];
            buffer.get(this.data);
        }
        checkArgument(index < this.offsets.length && index >= 0);
        int last = this.offsets.length - 1;
        int off = this.offsets[index];
        int end = index == last ? this.data.length : this.offsets[index + 1];
        int len = end - off;
        byte[] tmp = new byte[len];
        System.arraycopy(this.data, off, tmp, 0, len);
        return new String(tmp, charset);
    }
}
