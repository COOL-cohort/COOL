package com.nus.cool.core.io.storevector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.nus.cool.core.io.compression.Compressor;
import com.nus.cool.core.io.compression.ZIntCompressor;
import com.nus.cool.core.schema.Codec;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ZIntStoreTest {
  
  @Test
  public void test() {
    int[] numbers = {0, 1, 2, 3};
    // create compressor
    Compressor compressor = new ZIntCompressor(Codec.INT8, numbers.length);

    // compress the bytes
    int maxLen = compressor.maxCompressedLength();
    byte[] compressed = new byte[maxLen];
    compressor.compress(numbers, 0, numbers.length, compressed, 0, maxLen);
    
    // load the bytes with
    ByteBuffer buffer = ByteBuffer.wrap(compressed);
    buffer.order(ByteOrder.nativeOrder());
    int count = buffer.getInt();
    InputVector in = (InputVector) ZInt8Store.load(buffer, count);

    for (int i = 0; i < in.size(); i++) {
      Assert.assertEquals(in.get(i), i);
    }
    for (int i = 0; i < in.size(); i++) {
      Assert.assertEquals(in.find(i), i);
    }
  }
}
