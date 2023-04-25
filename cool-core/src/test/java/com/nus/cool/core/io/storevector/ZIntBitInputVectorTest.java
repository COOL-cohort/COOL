package com.nus.cool.core.io.storevector;

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.ValueWrapper;
import com.nus.cool.core.io.compression.Compressor;
import com.nus.cool.core.io.compression.CompressorOutput;
import com.nus.cool.core.io.compression.ZIntBitCompressor;
import com.nus.cool.core.util.ArrayUtil;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Testing ZIntBit input vector.
 */
public class ZIntBitInputVectorTest {
  @Test(dataProvider = "ZIntBitDP")
  public void zintbitinputvectorUnitTest(int[] numbers) {
    int max = ArrayUtil.max(numbers);
    // int count = numbers.length;

    List<FieldValue> values = IntStream.of(numbers)
        .mapToObj(x -> ValueWrapper.of(x))
        .collect(Collectors.toList());

    Compressor compressor = new ZIntBitCompressor(ValueWrapper.of(max));
    CompressorOutput compressed = compressor.compress(values);
    ByteBuffer buffer = ByteBuffer.wrap(compressed.getBuf(), 0, compressed.getLen());
    ZIntBitInputVector in = ZIntBitInputVector.load(buffer);

    for (int i = 0; i < in.size(); i++) {
      Assert.assertEquals(in.get(i).intValue(), numbers[i]);
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
