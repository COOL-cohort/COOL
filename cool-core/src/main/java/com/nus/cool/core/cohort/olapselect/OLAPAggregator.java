package com.nus.cool.core.cohort.olapselect;

import com.nus.cool.core.cohort.aggregate.AggregateFactory;
import com.nus.cool.core.cohort.aggregate.AggregateFunc;
import com.nus.cool.core.cohort.aggregate.AggregateType;
import com.nus.cool.core.cohort.storage.OLAPRet;
import com.nus.cool.core.cohort.storage.ProjectedTuple;
import com.nus.cool.core.cohort.storage.RetUnit;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.FieldType;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * OLAPAggregator.
 */
public class OLAPAggregator {

  /**
   * Process.
   *
   * @param aggregation        aggregation.
   * @param projectedSchemaSet projectedSchemaSet.
   * @return array of OLAPRet.
   */
  public ArrayList<OLAPRet> process(MetaChunkRS metaChunk, ChunkRS dataChunk,
                                    AggregationLayout aggregation,
                                           HashSet<String> projectedSchemaSet,
                                           Map<String, BitSet> group) {

    // init projectedTuple.
    ProjectedTuple tuple = new ProjectedTuple(projectedSchemaSet);

    // 1. get the field type and data
    String fieldName = aggregation.getFieldName();
    FieldType fieldType = metaChunk.getMetaField(fieldName).getFieldType();

    FieldRS field = dataChunk.getField(fieldName);

    // 2. init the Aggregate function
    Map<AggregateType, AggregateFunc> aggMap = new HashMap<>();
    for (AggregateType operator : aggregation.getOperators()) {
      if (!checkOperatorIllegal(fieldType, operator)) {
        throw new IllegalArgumentException(fieldName + " can not process " + operator);
      }
      AggregateFunc aggregator = AggregateFactory.generateAggregate(operator, fieldName);
      aggMap.put(operator, aggregator);
    }

    // 3. init the result
    Map<String, Map<AggregateType, RetUnit>> resultMap = new HashMap<>();

    // 4. traverse once and conduct all operations
    for (Map.Entry<String, BitSet> entry : group.entrySet()) {
      String groupName = entry.getKey();
      BitSet groupBs = entry.getValue();

      Map<AggregateType, RetUnit> res = new HashMap<>();
      resultMap.put(groupName, res);

      for (int i = 0; i < groupBs.size(); i++) {
        int nextpos = groupBs.nextSetBit(i);
        if (nextpos < 0) {
          break;
        }

        FieldValue value = field.getValueByIndex(nextpos);
        tuple.loadAttr(value, fieldName);
        // for each operator.
        for (AggregateType operator : aggregation.getOperators()) {
          // init result into dict
          resultMap.get(groupName).putIfAbsent(operator, new RetUnit(0, 0));
          // do aggregation
          AggregateFunc aggregator = aggMap.get(operator);
          aggregator.calculate(resultMap.get(groupName).get(operator), tuple);
        }
        i = nextpos;
      }
    }

    ArrayList<OLAPRet> results = new ArrayList<>();

    for (Map.Entry<String, Map<AggregateType, RetUnit>> entry : resultMap.entrySet()) {
      String groupName = entry.getKey();
      Map<AggregateType, RetUnit> groupValue = entry.getValue();

      // assign new result
      OLAPRet newEle = new OLAPRet();
      // newEle.setTimeRange(this.timeRange);
      newEle.setKey(groupName);
      newEle.setFieldName(fieldName);
      newEle.initAggregator(groupValue);
      results.add(newEle);
    }
    return results;
  }


  private boolean checkOperatorIllegal(FieldType fieldType, AggregateType aggregatorType) {
    switch (aggregatorType) {
      case SUM:
      case AVERAGE:
      case MAX:
      case MIN:
        return fieldType.equals(FieldType.Metric);
      case COUNT:
        return true;
      case DISTINCT:
        return !fieldType.equals(FieldType.Metric);
      default:
        throw new UnsupportedOperationException();
    }
  }
}
