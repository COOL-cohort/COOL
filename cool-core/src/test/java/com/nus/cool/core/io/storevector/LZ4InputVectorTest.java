package com.nus.cool.core.io.storevector;

import com.nus.cool.core.io.DataOutputBuffer;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.LZ4JavaCompressor;
import com.nus.cool.core.schema.CompressType;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Testing LZ4 input vector.
 */
public class LZ4InputVectorTest {

  static final Logger logger = LoggerFactory.getLogger(LZ4InputVectorTest.class);
  static final Charset defaultCharset = Charset.defaultCharset();

  @BeforeTest
  public void setUp() {
    logger.info("Start UnitTest " + LZ4InputVectorTest.class.getSimpleName());
  }

  @AfterTest
  public void tearDown() {
    logger.info(String.format("Tear Down UnitTest %s\n", LZ4InputVectorTest.class.getSimpleName()));
  }

  @Test(dataProvider = "LZ4InputVectorDP", enabled = true)
  public void lz4InputVectorUnitTest(String[] valueList) throws IOException {
    logger.info(
        String.format("Input LZ4InputVector UnitTest Data: ValueList Size %d", valueList.length));

    DataOutputBuffer buf = new DataOutputBuffer();
    // Write Size
    buf.writeInt(valueList.length);
    // Write Offset of Value and string value
    int offset = 0;
    for (String v : valueList) {
      buf.writeInt(offset);
      offset += v.getBytes().length;
    }

    for (String v : valueList) {
      buf.write(v.getBytes());
    }
    // Use LZ4JavaCompressor to Compress
    Histogram hist = Histogram.builder().type(CompressType.KeyString).rawSize(buf.size()).build();
    LZ4JavaCompressor compressor = new LZ4JavaCompressor(hist);
    int maxLen = compressor.maxCompressedLength();
    byte[] compressed = new byte[maxLen];
    compressor.compress(buf.getData(), 0, buf.size(), compressed, 0, maxLen);

    buf.close();

    // Decoding these readBuf
    ByteBuffer readBuf = ByteBuffer.wrap(compressed);
    readBuf.order(ByteOrder.nativeOrder());
    LZ4InputVector res = new LZ4InputVector(defaultCharset);
    res.readFrom(readBuf);

    // Validate these string
    for (int i = 0; i < valueList.length; i++) {
      String actual = res.get(i);
      String expect = String.valueOf(valueList[i]);
      if (!expect.equals(actual)) {
        logger.debug(String.format("Actual %s ,l:%d", actual, actual.length()));
        logger.debug(String.format("Expected %s, l:%d", expect, expect.length()));
      }
      Assert.assertEquals(expect, actual);
    }
  }

  /**
   * Data provider.
   */
  @DataProvider(name = "LZ4InputVectorDP", parallel = false)
  public Object[][] lz4InputVectorDP() {
    return new Object[][] { { generateValueList(10) }, { generateValueList(100) },
        { new String[] { "111", "222", "333", "555", "KKK", "111" } }, };
  }

  private String[] generateValueList(int n) {
    String[] res = new String[n];
    for (int i = 0; i < n; i++) {
      res[i] = "value_" + i;
    }
    return res;
  }
}
