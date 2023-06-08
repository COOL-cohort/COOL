package com.nus.cool.core.io.store;

import com.nus.cool.core.io.DataOutputBuffer;
import com.nus.cool.core.io.readstore.CubeMetaRS;
import com.nus.cool.core.io.writestore.MetaChunkWS;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.TableSchema;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Testing cube meta.
 */
public class CubeMetaTest {
  static final Logger logger = LoggerFactory.getLogger(CubeMetaTest.class);
  private TableSchema schema;
  private TestTable table;
  private MetaChunkWS metaws;

  private String[] expected = {
    "{\"charset\":\"UTF-8\",\"type\":\"UserKey\",\"values\":[\"P-0\",\"P-1\",\"P-10\",\"P-11\","
      + "\"P-12\",\"P-2\",\"P-3\",\"P-4\",\"P-5\",\"P-6\",\"P-7\",\"P-8\",\"P-9\"]}",
    "{\"type\":\"Metric\",\"min\":\"1954\",\"max\":\"2000\"}",
    "{\"charset\":\"UTF-8\",\"type\":\"Action\",\"values\":[\"diagnose\",\"labtest\","
      + "\"prescribe\"]}",
    "{\"charset\":\"UTF-8\",\"type\":\"Segment\",\"values\":[\"Disease-A\",\"Disease-B\","
      + "\"Disease-C\",\"None\"]}",
    "{\"charset\":\"UTF-8\",\"type\":\"Segment\",\"values\":[\"Medicine-A\",\"Medicine-B\","
      + "\"Medicine-C\",\"None\"]}",
    "{\"charset\":\"UTF-8\",\"type\":\"Segment\",\"values\":[\"Labtest-A\",\"Labtest-B\","
      + "\"Labtest-C\",\"None\"]}",
    "{\"type\":\"Float\",\"min\":\"0.0\",\"max\":\"70.0\"}",
    "{\"type\":\"ActionTime\",\"min\":\"" + Integer.toString(15340 * 24 * 3600)
      + "\",\"max\":\"" +  Integer.toString(15696 * 24 * 3600) + "\"}"
  };

  /**
   * setup.
   */
  @BeforeTest
  public void setup() throws IOException {
    logger.info("Start UnitTest " + CubeMetaTest.class.getSimpleName());
    String sourcePath = Paths.get(System.getProperty("user.dir"), "src", "test", "java", "com",
        "nus", "cool", "core", "resources").toString();

    String dirPath = Paths.get(sourcePath, "health").toString();
    this.table = Utils.loadTable(dirPath);
    this.schema = table.getSchema();

    this.metaws = MetaChunkWS.newMetaChunkWS(this.schema, 0);
  }

  @AfterTest
  public void tearDown() {
    logger.info(String.format("Tear down UnitTest %s\n", CubeMetaTest.class.getSimpleName()));
  }

  @Test
  public void cubeMetaUnitTest() throws IOException {
    for (int i = 0; i < this.table.getRowCounts(); i++) {
      metaws.put(this.table.getTuple(i));
    }

    DataOutputBuffer out = new DataOutputBuffer();
    metaws.complete();
    int written = metaws.writeCubeMeta(out);
    out.close();

    ByteBuffer buffer = ByteBuffer.wrap(out.getData());
    buffer.limit(written);

    CubeMetaRS cubemeta = new CubeMetaRS(this.schema);
    cubemeta.readFrom(buffer);
    int idx = 0;
    for (FieldSchema field : schema.getFields()) {
      String fieldmeta = cubemeta.getFieldMeta(field.getName());
      Assert.assertEquals(fieldmeta, expected[idx++]);
    }
  }
}
