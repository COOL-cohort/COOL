package com.nus.cool.core.refactor;

import com.nus.cool.core.cohort.refactor.filter.Filter;
import com.nus.cool.core.cohort.refactor.filter.FilterLayout;
import com.nus.cool.core.cohort.refactor.filter.RangeFilter;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class FilterTest {
  @Test(dataProvider = "RangeFilterAcceptDP")
  public void rangeFilterAcceptUnitTest(FilterLayout fLayout, int[] inputs) {
    RangeFilter filter = (RangeFilter) fLayout.generateFilter();
    for (int i = 0; i < inputs.length; i++) {
      Assert.assertTrue(filter.accept(inputs[i]));
    }
  }

  @Test(dataProvider = "RangeFilterRejectDP")
  public void rangeFilterRejectUnitTest(FilterLayout fLayout, int[] inputs) {
    RangeFilter filter = (RangeFilter) fLayout.generateFilter();
    for (int i = 0; i < inputs.length; i++) {
      Assert.assertFalse(filter.accept(inputs[i]));
    }
  }

  @Test(dataProvider = "SetFilterAcceptDP")
  public void setFilterAcceptUnitTest(FilterLayout flayout, String[] inputs) {
    Filter filter = flayout.generateFilter();
    for (int i = 0; i < inputs.length; i++) {
      Assert.assertTrue(filter.accept(inputs[i]));
    }
  }

  @Test(dataProvider = "SetFilterRejectDP")
  public void setFilterRejectUnitTest(FilterLayout flayout, String[] inputs) {
    Filter filter = flayout.generateFilter();
    for (int i = 0; i < inputs.length; i++) {
      Assert.assertFalse(filter.accept(inputs[i]));
    }
  }


  @DataProvider(name = "SetFilterAcceptDP")
  public Object[][] generateSetFilterAcceptInput() {
    return new Object[][] {
        {new FilterLayout(true, new String[] {"A", "B", "C", "D", "E"}, null),
            new String[] {"A", "A", "B", "B", "D", "E", "C", "C"},
        },
        {
            new FilterLayout(true, null, new String[] {"11", "222", "3333", "44444", "555555"}),
            new String[] {"1111", "222222", "33", "44", "55", "22222"}
        }
    };
  }

  @DataProvider(name = "SetFilterRejectDP")
  public Object[][] generateSetFilterRejectInput() {
    return new Object[][] {
        {
            new FilterLayout(true, new String[] {"A", "B", "C", "D", "E"}, null),
            new String[] {"111", "CCCCC", "KKK", "MMM", "12334"},
        },

        {
            new FilterLayout(true, null, new String[] {"11", "222", "3333", "44444", "555555"}),
            new String[] {"11", "222", "3333", "44444", "555555"}
        }
    };
  }

  @DataProvider(name = "RangeFilterAcceptDP")
  public Object[][] generateRangeFilterAcceptInput() {
    return new Object[][] {
        {
            new FilterLayout(false, new String[] {"MIN-15", "16-2000", "2555-MAX"}, null),
            new int[] {12, 55, 555555, 1999, 14, 16, 1999, 2555}
        }
    };
  }

  @DataProvider(name = "RangeFilterRejectDP")
  public Object[][] generateRangeFilterRejectInput() {
    return new Object[][] {
        {
            new FilterLayout(false, new String[] {"MIN-15", "16-2000", "2555-MAX"}, null),
            new int[] {2001, 2222, 2244}
        }
    };
  }

};