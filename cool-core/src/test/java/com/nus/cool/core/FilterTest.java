package com.nus.cool.core;

import com.nus.cool.core.cohort.filter.Filter;
import com.nus.cool.core.cohort.filter.FilterLayout;
import com.nus.cool.core.cohort.filter.RangeFilter;
import com.nus.cool.core.field.IntRangeField;
import com.nus.cool.core.field.StringHashField;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Testing cohort filters.
 */
public class FilterTest {
  @Test(dataProvider = "RangeFilterAcceptDP")
  public void rangeFilterAcceptUnitTest(FilterLayout fLayout, int[] inputs) {
    RangeFilter filter = (RangeFilter) fLayout.generateFilter();
    for (int i = 0; i < inputs.length; i++) {
      Assert.assertTrue(filter.accept(new IntRangeField(inputs[i])));
    }
  }

  @Test(dataProvider = "RangeFilterRejectDP")
  public void rangeFilterRejectUnitTest(FilterLayout fLayout, int[] inputs) {
    RangeFilter filter = (RangeFilter) fLayout.generateFilter();
    for (int i = 0; i < inputs.length; i++) {
      Assert.assertFalse(filter.accept(new IntRangeField(inputs[i])));
    }
  }

  @Test(dataProvider = "SetFilterAcceptDP")
  public void setFilterAcceptUnitTest(FilterLayout flayout, String[] inputs) {
    Filter filter = flayout.generateFilter();
    for (int i = 0; i < inputs.length; i++) {
      Assert.assertTrue(filter.accept(new StringHashField(inputs[i])));
    }
  }

  @Test(dataProvider = "SetFilterRejectDP")
  public void setFilterRejectUnitTest(FilterLayout flayout, String[] inputs) {
    Filter filter = flayout.generateFilter();
    for (int i = 0; i < inputs.length; i++) {
      Assert.assertFalse(filter.accept(new StringHashField(inputs[i])));
    }
  }

  /**
   * Data provider for set filter acceptance.
   */
  @DataProvider(name = "SetFilterAcceptDP")
  public Object[][] generateSetFilterAcceptInput() {
    return new Object[][] {
        {
            new FilterLayout(true, new String[] { "A", "B", "C", "D", "E" }, null),
            new String[] { "A", "A", "B", "B", "D", "E", "C", "C" },
        },
        {
            new FilterLayout(true, null, new String[] { "11", "222", "3333", "44444", "555555" }),
            new String[] { "1111", "222222", "33", "44", "55", "22222" } }
        };
  }

  /**
   * Data provider for set filter rejection.
   */
  @DataProvider(name = "SetFilterRejectDP")
  public Object[][] generateSetFilterRejectInput() {
    return new Object[][] {
        {
            new FilterLayout(true, new String[] { "A", "B", "C", "D", "E" }, null),
            new String[] { "111", "CCCCC", "KKK", "MMM", "12334" },
        },

        {
            new FilterLayout(true, null, new String[] { "11", "222", "3333", "44444", "555555" }),
            new String[] { "11", "222", "3333", "44444", "555555" } }
        };
  }

  /**
   * Data provider for range filter acceptance.
   */
  @DataProvider(name = "RangeFilterAcceptDP")
  public Object[][] generateRangeFilterAcceptInput() {
    return new Object[][] {
        {
            new FilterLayout(false, new String[] { "MIN to 15", "16 to 2000", "2555 to MAX" },
                null),
            new int[] { 12, 55, 555555, 1999, 14, 16, 1999, 2555 } }
        };
  }

  /**
   * Data provider for range filter rejection.
   */
  @DataProvider(name = "RangeFilterRejectDP")
  public Object[][] generateRangeFilterRejectInput() {
    return new Object[][] {
        {
            new FilterLayout(false, new String[] { "MIN to 15", "16 to 2000", "2555 to MAX" },
                null),
            new int[] { 2001, 2222, 2244 } }
        };
  }
}
