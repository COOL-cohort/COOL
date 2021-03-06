package com.nus.cool.core.io.storevector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.nus.cool.core.io.compression.BitVectorCompressor;
import com.nus.cool.core.io.compression.Compressor;

import org.testng.Assert;
import org.testng.annotations.Test;

public class BitVectorInputVectorTest {
  
  @Test
  public void test() {
    int[] numbers = {0, 1, 2, 3};
    Compressor compressor = new BitVectorCompressor(3);
    // compress the bytes
    int maxLen = compressor.maxCompressedLength();
    byte[] compressed = new byte[maxLen];
    compressor.compress(numbers, 0, numbers.length, compressed, 0, maxLen);
    
    // load the bytes with
    ByteBuffer buffer = ByteBuffer.wrap(compressed);
    buffer.order(ByteOrder.nativeOrder());
    InputVector in = new BitVectorInputVector();
    in.readFrom(buffer);

    for (int i = 0; i < in.size(); i++) {
      Assert.assertEquals(in.get(i), i);
    }
    for (int i = 0; i < in.size(); i++) {
      Assert.assertEquals(in.find(i), i);
    }
  }
}
