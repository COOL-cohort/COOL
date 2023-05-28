package com.nus.cool.core.cohort.storage;

import com.nus.cool.core.field.FieldValue;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Partial of a tuple in project, when generated, the layout of schemas is
 * fixed. Instance of
 * ProjectTuple can load Object[] to reuse this memory
 */
public class ProjectedTuple {

  private FieldValue[] tuple;

  private Map<String, Integer> schema2Index;

  /**
   * Get the value by field name.
   */
  public FieldValue getValueBySchema(String schema) {
    if (!schema2Index.containsKey(schema)) {
      return null;
    }
    return tuple[schema2Index.get(schema)];
  }

  /**
   * Create a partial tuple with selected fields.
   */
  public ProjectedTuple(Set<String> schemaList) {
    this.schema2Index = new HashMap<>();
    int idx = 0;
    for (String schema : schemaList) {
      this.schema2Index.put(schema, idx++);
    }
    this.tuple = new FieldValue[this.schema2Index.size()];
  }

  // public ProjectedTuple

  // /**
  // * guarante the value in load tuple is ordered according to schemas' order.
  // * @param tuple
  // */
  // public void loadTuple(Object[] tuple){
  // this.tuple = tuple;
  // }

  public void loadAttr(FieldValue attrValue, String schema) {
    int idx = this.schema2Index.get(schema);
    this.tuple[idx] = attrValue;
  }

  /**
   * Return the layout of this ProjectedTuple.
   */
  public String[] getSchemaList() {
    String[] ret = new String[schema2Index.size()];
    for (Entry<String, Integer> entry : schema2Index.entrySet()) {
      ret[entry.getValue()] = entry.getKey();
    }
    return ret;
  }
}
