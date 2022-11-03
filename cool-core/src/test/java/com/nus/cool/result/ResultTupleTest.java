package com.nus.cool.result;

import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Testing result tuple.
 */
public class ResultTupleTest {
  @Test(dataProvider = "ResultTupleDP")
  public void mergeTest(ResultTuple[] resTuples) {

    List<ResultTuple> resList1 = new ArrayList<ResultTuple>();
    resList1.add(resTuples[0]);
    resList1.add(resTuples[1]);
    List<ResultTuple> output1 = ResultTuple.merge(resList1);
    Assert.assertEquals(output1.size(), 2);

    // merge same cohort key, same age
    List<ResultTuple> resList2 = new ArrayList<ResultTuple>();
    resList2.add(resTuples[1]);
    resList2.add(resTuples[2]);
    List<ResultTuple> output2 = ResultTuple.merge(resList2);
    Assert.assertNotEquals(output2.size(), 2);
    Assert.assertEquals(output2.size(), 1);
  }

  /**
   * Data provider.
   */
  @DataProvider(name = "ResultTupleDP")
  public Object[][] dpArgs() {
    ResultTuple res1 = new ResultTuple("ab", 7, 76L);
    ResultTuple res2 = new ResultTuple("bc", 8, 26L);
    ResultTuple res4 = new ResultTuple("bc", 8, 6L);
    return new Object[][] {
        {new ResultTuple[] {res1, res2, res4}}
    };
  }
}
