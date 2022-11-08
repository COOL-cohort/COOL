package com.nus.cool.core.cohort.filter;

import com.nus.cool.core.schema.TableSchema;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Testing range field filters.
 */
public class RangeFieldFilterTest {
  @Test
  public void rangeFilterUnitTest() {
    String schemaStr = "charset: \"UTF-8\"\n"
        + "fields:\n"
        + "- name: \"money\"\n"
        + "  fieldType: \"Metric\"\n"
        + "  preCal: false\n"
        + "- name: \"event\"\n"
        + "  fieldType: \"Action\"\n"
        + "  preCal: false\n"
        + "- name: \"eventDay\"\n"
        + "  fieldType: \"ActionTime\"\n"
        + "  preCal: false\n"
        + "- name: \"other\"\n"
        + "  fieldType: \"Segment\"\n"
        + "  preCal: false\n";

    try {
      InputStream in = new ByteArrayInputStream(
          schemaStr.getBytes("UTF-8"));
      TableSchema tschema = TableSchema.read(in);

      List<String> values = new ArrayList<>();
      values.add("50|90");
      values.add("10|20");
      FieldFilter fieldFilter = FieldFilterFactory.create(tschema.getFieldSchema("money"),
          null, values);
      Assert.assertEquals(fieldFilter.getClass(), RangeFieldFilter.class);
      Assert.assertEquals(fieldFilter.getMaxKey(), 90);
      Assert.assertEquals(fieldFilter.getMinKey(), 10);
      Assert.assertTrue(fieldFilter.accept(15));
      Assert.assertTrue(fieldFilter.accept(60));
      Assert.assertFalse(fieldFilter.accept(30));
    } catch (IOException e) {
      System.err.println(e.getStackTrace());
    }
  }
}
