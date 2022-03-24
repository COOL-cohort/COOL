package com.nus.cool.core.io.storevector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Map;

import com.google.common.collect.Maps;
import com.nus.cool.core.io.DataOutputBuffer;
import com.nus.cool.core.io.compression.Compressor;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.LZ4JavaCompressor;
import com.nus.cool.core.schema.CompressType;

import org.testng.Assert;
import org.testng.annotations.Test;

public class LZ4InputVectorTest {

  @Test
  public void testRead() {
    DataOutputBuffer buffer = new DataOutputBuffer();
    Map<Integer, String> dict = Maps.newTreeMap();
    for (int i = 0; i < 5; i++) {
      dict.put(i, "value" + i);
    }
    try {
      buffer.writeInt(dict.size());
      int off = 0;
      for (Map.Entry<Integer, String> en: dict.entrySet()) {
        buffer.writeInt(off);
        off += en.getValue().getBytes().length;
      }
      
      Charset charset = Charset.forName("UTF-8");
      for (Map.Entry<Integer, String> en: dict.entrySet()) {
        buffer.write(en.getValue().getBytes(charset));
      }
      
      // build histogram
      Histogram hist = Histogram.builder().type(CompressType.KeyString)
                                .rawSize(buffer.size()).build();

      // create compressor
      Compressor compressor = new LZ4JavaCompressor(hist);

      // compress the bytes
      int maxLen = compressor.maxCompressedLength();
      byte[] compressed = new byte[maxLen];
      compressor.compress(buffer.getData(), 0, buffer.size(), compressed, 0, maxLen);
      
      LZ4InputVector in = new LZ4InputVector();
      ByteBuffer compressed_buffer = ByteBuffer.wrap(compressed);
      compressed_buffer.order(ByteOrder.nativeOrder());
      in.readFrom(compressed_buffer);
      
      Assert.assertEquals(in.getString(0, charset), "value0");
      Assert.assertEquals(in.getString(2, charset), "value2");

      // load the bytes with
    } catch (IOException e) {
      System.out.println("IOException encountered");
    }
    Assert.assertTrue(true);
  }
}
