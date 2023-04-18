package com.nus.cool.core.io.storevector;

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.ValueWrapper;
import com.nus.cool.core.io.compression.Compressor;
import com.nus.cool.core.io.compression.CompressorOutput;
import com.nus.cool.core.io.compression.ZIntCompressor;
import com.nus.cool.core.schema.Codec;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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

    List<FieldValue> values = IntStream.of(numbers)
        .mapToObj(x -> ValueWrapper.of(x))
        .collect(Collectors.toList());

    Compressor compressor = new ZIntCompressor(codeType, false);
    CompressorOutput compressed = compressor.compress(values);

    // load the bytes with
    ByteBuffer buffer = ByteBuffer.wrap(compressed.getBuf(), 0, compressed.getLen());
    ZIntStore store;

    switch (codeType) {
      case INT8:
        store = new ZInt8Store();
        store.readFrom(buffer);
        break;
      case INT16:
        store = new ZInt16Store();
        store.readFrom(buffer);
        break;
      case INT32:
        // INT32
        store = new ZInt32Store();
        store.readFrom(buffer);
        break;
      default:
        throw new IllegalArgumentException("Invalid INT code Type");
    }

    for (int i = 0; i < store.size(); i++) {
      Assert.assertEquals(store.get(i).intValue(), numbers[i]);
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
}
