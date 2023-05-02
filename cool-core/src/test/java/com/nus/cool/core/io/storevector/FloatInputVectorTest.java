package com.nus.cool.core.io.storevector;

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.ValueWrapper;
import com.nus.cool.core.io.compression.Compressor;
import com.nus.cool.core.io.compression.CompressorOutput;
import com.nus.cool.core.io.compression.FloatCompressor;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Testing Float compression and decompression.
 */
public class FloatInputVectorTest {
  static final Logger logger = LoggerFactory.getLogger(FloatInputVectorTest.class.getSimpleName());

  @BeforeTest
  public void setUp() {
    logger.info("Start UnitTest " + FloatInputVectorTest.class.getSimpleName());
  }

  @AfterTest
  public void tearDown() {
    logger.info(String.format("Tear Down UnitTest %s\n",
        FloatInputVectorTest.class.getSimpleName()));
  }

  @Test(dataProvider = "FloatCompressorDP")
  public void testFloat(Float[] numbers) throws Exception {
    logger.info(String.format("Input FloatInputVector UnitTest Data:", numbers.toString()));

    List<FieldValue> values = Stream.of(numbers)
        .map(x -> ValueWrapper.of(x))
        .collect(Collectors.toList());

    Compressor compressor = new FloatCompressor();
    CompressorOutput compressed = compressor.compress(values);

    // load the bytes with
    ByteBuffer buffer = ByteBuffer.wrap(compressed.getBuf(), 0, compressed.getLen());
    
    FloatInputVector iv = new FloatInputVector();
    iv.readFrom(buffer);

    for (int i = 0; i < iv.size(); i++) {
      Assert.assertEquals(iv.get(i).floatValue(), (float) numbers[i]);
    }
  }

  /**
   * Data provider.
   */
  @DataProvider(name = "FloatCompressorDP")
  public Object[][] dpArgs() {
    return new Object[][] {
        { new Float[] { 10.3f, 1.2f, 1.2f, 1.3f, 1.4f, 5.2f, 6.1f, 1.000f}}, // test case 3
        { new Float[] { 15.5f, 135296f, 132096f, 136320f, 132352f}}, // test case 2
        { new Float[] { 10.3f, 1.2f, 1.2f, 1.2f, 1.2f, 1.2f, 1.2f, 1.2f, 1.2f,
            1.2f}}, // test case 1 with many equal values
        { new Float[] { -1.0f, -1.0f, -1.0f, -1.0f, 29.60f, 6.70f,
            60.5f}}, // test case with xor 32 bit value
    };
  }
}
