package com.nus.cool.core.cohort;

import com.nus.cool.core.cohort.aggregator.*;

import java.util.HashMap;
import java.util.Map;

public class BirthAggregatorFactory {

    private static final Map<String, EventAggregator> aggregators = new HashMap<>();
    
    static {
        aggregators.put("UNIQUE", new UniqueAggregator());
    	aggregators.put("COUNT", new BirthCountAggregator());
    	aggregators.put("RETENTION", new UserCountAggregatorEvent());
    	aggregators.put("SUM", new BirthSumAggregator());
    	aggregators.put("AVG", new BirthAvgAggregator());
    	aggregators.put("MAX", new BirthMaxAggregator());
    	aggregators.put("MIN", new BirthMinAggregator());
    	aggregators.put("ROLLRETENTION", new RollingRentionAggregator());
    }

    public static EventAggregator getAggregator(String op) {
        return aggregators.get(op);
    }
}
