package com.nus.cool.core.io.storevector;

import com.nus.cool.core.io.compression.Compressor;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.ZIntCompressor;
import com.nus.cool.core.schema.Codec;
import com.nus.cool.core.schema.CompressType;
import com.nus.cool.core.util.ArrayUtil;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Testing ZIntStore.
 */
public class ZIntStoreTest {

  static final Logger logger = LoggerFactory.getLogger(ZIntStoreTest.class.getSimpleName());

  @BeforeTest
  public void setUp() {
    logger.info("Start UnitTest " + ZIntStoreTest.class.getSimpleName());
  }

  @AfterTest
  public void tearDown() {
    logger.info(String.format("Tear Down UnitTest %s\n", ZIntStoreTest.class.getSimpleName()));
  }

  @Test(dataProvider = "ZIntStoreDP")
  public void testZIntStore(int[] numbers, Codec codeType) throws Exception {
    logger.info(String.format("Input ZIntStore UnitTest Data: Code Type %s Input Data %s",
        codeType.name(), numbers.toString()));

    int min = ArrayUtil.min(numbers);
    int max = ArrayUtil.max(numbers);
    int count = numbers.length;
    int rawSize = count * codecByte(codeType);

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
    ZIntStore store;

    switch (codeType) {
      case INT8:
        store = ZInt8Store.load(buffer);
        break;
      case INT16:
        store = ZInt16Store.load(buffer);
        break;
      case INT32:
        // INT32
        store = ZInt32Store.load(buffer);
        break;
      default:
        throw new IllegalArgumentException("Invalid INT code Type");
    }

    InputVector in = (InputVector) store;

    for (int i = 0; i < in.size(); i++) {
      Assert.assertEquals(in.get(i), numbers[i]);
    }
  }

  /**
   * Data provider.
   */
  @DataProvider(name = "ZIntStoreDP")
  public Object[][] dpArgs() {
    return new Object[][] {
        { new int[] { 0, 1, 2, 3, 4, 5, 6, 1000, 2222, 9499 }, Codec.INT32 },
        { new int[] { 1, 5, 666, 777, 888, 999, 222 }, Codec.INT16 },
        { new int[] { 1, 7, 12, 11, 12, 44 }, Codec.INT8 },
    };
  }

  private int codecByte(Codec ctype) {
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