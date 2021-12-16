package com.nus.cool.loader;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
public class ResultTuple {

    private String cohort;

    private int age;

    private long measure;

    public static List<ResultTuple> merge(List<ResultTuple> resultTuples) {
        Map<String, Map<Integer, Long>> resMap = new HashMap<>();
        for (ResultTuple res : resultTuples) {
            if (resMap.get(res.cohort) == null) {
                Map<Integer, Long> map = new HashMap<>();
                map.put(res.age, res.measure);
                resMap.put(res.cohort, map);
            } else if (resMap.get(res.cohort).get(res.age) == null) {
                Map<Integer, Long> map = resMap.get(res.cohort);
                map.put(res.age, res.measure);
            } else {
                Map<Integer, Long> map = resMap.get(res.cohort);
                map.put(res.age, res.measure + map.get(res.age));
            }
        }
        List<ResultTuple> results = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, Long>> entry : resMap.entrySet()) {
            String cohort = entry.getKey();
            Map<Integer, Long> map = entry.getValue();
            for (Map.Entry<Integer, Long> entry1 : map.entrySet()) {
                int age = entry1.getKey();
                long measure = entry1.getValue();
                ResultTuple res = new ResultTuple(cohort, age, measure);
                results.add(res);
            }
        }
        return results;
    }
}
