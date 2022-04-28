package com.nus.cool.core.io.storevector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import com.nus.cool.core.io.compression.Compressor;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.ZIntCompressor;
import com.nus.cool.core.schema.Codec;
import com.nus.cool.core.schema.CompressType;
import com.nus.cool.core.util.ArrayUtil;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ZIntStoreTest {

  @BeforeTest
  public void setUp() {
    System.out.println("ZIntStore UnitTest");
  }

  @Test(dataProvider = "ZIntStoreDP")
  public void TestZIntStore(int[] numbers, Codec codeType) throws Exception {

    System.out.printf("ZIntStoreDP UnitTest Input: numbers %s INTType %s\n", Arrays.toString(numbers), codeType);
    int min = ArrayUtil.min(numbers);
    int max = ArrayUtil.max(numbers);
    int count = numbers.length;
    int rawSize = count * CodecByte(codeType);

    Histogram hist = Histogram.builder()
        .min(min)
        .max(max)
        .numOfValues(count)
        .rawSize(rawSize)
        .type(CompressType.KeyHash)
        .build();
    Compressor compressor = new ZIntCompressor(codeType, hist);
    int maxLen = compressor.maxCompressedLength();
    byte[] compressed = new byte[maxLen];
    compressor.compress(numbers, 0, count, compressed, 0, maxLen);

    // load the bytes with
    ByteBuffer buffer = ByteBuffer.wrap(compressed);
    buffer.order(ByteOrder.nativeOrder());
    int sz = buffer.getInt();
    ZIntStore store;

    switch (codeType) {
      case INT8:
        store = new ZInt8Store(sz);
        break;
      case INT16:
        store = new ZInt16Store(sz);
        break;
      case INT32:
        // INT32
        store = new ZInt32Store(sz);
        break;
      default:
        throw new IllegalArgumentException("Invalid INT code Type");
    }
    store.readFrom(buffer);
    InputVector in = (InputVector) store;

    for (int i = 0; i < in.size(); i++) {
      Assert.assertEquals(in.get(i), numbers[i]);
    }
  }

  @DataProvider(name = "ZIntStoreDP")
  public Object[][] dpArgs() {
    return new Object[][] {
        { new int[] { 0, 1, 2, 3, 4, 5, 6, 1000, 2222, 9499 }, Codec.INT32 },
        { new int[] { 1, 5, 666, 777, 888, 999, 222 }, Codec.INT16 },
        { new int[] { 1, 7, 12, 11, 12, 44 }, Codec.INT8 },
    };
  }

  private int CodecByte(Codec ctype) {
    switch (ctype) {
      case INT32:
        return 4;
      case INT16:
        return 2;
      case INT8:
        return 1;
      default:
        throw new IllegalArgumentException("Invalid INT code Type");
    }
  }
}