package com.nus.cool.result;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;


public class ResultTupleTest {
    @Test
    public void mergeTest() {
        ResultTuple res_1 = new ResultTuple("ab",7,76L);
        ResultTuple res_2 = new ResultTuple("bc",8,26L);
        ResultTuple res_4 = new ResultTuple( "bc",8,6L);

        List<ResultTuple>  res_list1 = new ArrayList<ResultTuple>();
        res_list1.add(res_1);
        res_list1.add(res_2);
        List<ResultTuple> output1 = ResultTuple.merge(res_list1);
        Assert.assertEquals(output1.size(), 2);

        //merge same cohort key, same age
        List<ResultTuple>  res_list2 = new ArrayList<ResultTuple>();
        res_list2.add(res_2);
        res_list2.add(res_4);
        List<ResultTuple> output2 = ResultTuple.merge(res_list2);
        Assert.assertNotEquals(output2.size(), 2);
        Assert.assertEquals(output2.size(), 1);


    }

}