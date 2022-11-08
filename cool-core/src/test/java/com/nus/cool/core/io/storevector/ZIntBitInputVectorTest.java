package com.nus.cool.core.io.storevector;

import com.nus.cool.core.io.compression.Compressor;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.ZIntBitCompressor;
import com.nus.cool.core.schema.CompressType;
import com.nus.cool.core.util.ArrayUtil;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Testing ZIntBit input vector.
 */
public class ZIntBitInputVectorTest {
  @Test(dataProvider = "ZIntBitDP")
  public void zintbitinputvectorUnitTest(int[] numbers) {
    int min = ArrayUtil.min(numbers);
    int max = ArrayUtil.max(numbers);
    int count = numbers.length;

    Histogram hist = Histogram.builder().min(min).max(max).numOfValues(count)
        .type(CompressType.KeyHash).build();
    Compressor compressor = new ZIntBitCompressor(hist);
    int maxLen = compressor.maxCompressedLength();
    byte[] compressed = new byte[maxLen];
    compressor.compress(numbers, 0, count, compressed, 0, maxLen);
    ByteBuffer buffer = ByteBuffer.wrap(compressed);
    buffer.order(ByteOrder.nativeOrder());
    ZIntBitInputVector in = ZIntBitInputVector.load(buffer);

    for (int i = 0; i < in.size(); i++) {
      Assert.assertEquals(in.get(i), numbers[i]);
    }

  }

  /**
   * Data provider.
   */
  @DataProvider(name = "ZIntBitDP")
  public Object[][] dpArgs() {
    return new Object[][] {
        { new int[] { 0, 0, 8, 3, 4, 5, 6, 1000, 2354, 9499 } },
        { new int[] { 1, 5, 233, 777, 333, 999, 3434 } }
    };
  }

}
