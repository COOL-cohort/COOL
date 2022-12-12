package com.nus.cool.core.cohort;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.olapselect.AggregationLayout;
import com.nus.cool.core.cohort.olapselect.OLAPSelectionLayout;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import lombok.Getter;


/**
 * Layout class to facilitate json serialization of olap query.
 */
@Getter
public class OLAPQueryLayout {

  /**
   *inner structure.
  */
  public enum GranularityType {
    DAY, MONTH, YEAR, NULL
  }

  // datasource name
  @JsonProperty("dataSource")
  private String dataSource;

  // select condition
  @JsonProperty("selection")
  private OLAPSelectionLayout selection;

  // a list a groupFields
  @JsonProperty("groupFields")
  private List<String> groupFields;

  // granularity for groupBy, if the groupBy field is dataType,
  @JsonProperty("groupFieldsGranularity")
  private GranularityType groupFieldsGranularity;

  // a list of aggregation functions, sum, count etc
  @JsonProperty("aggregations")
  private List<AggregationLayout> aggregations;

  // granularity for time range
  @JsonProperty("granularity")
  private GranularityType granularity;

  public static OLAPQueryLayout readFromJson(File inputFile) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(inputFile, OLAPQueryLayout.class);
  }

  public static OLAPQueryLayout readFromJson(String path) throws IOException {
    return readFromJson(new File(path));
  }

  /**
   * Return the schema set.
   */
  public HashSet<String> getSchemaSet() {

    HashSet<String> ret = new HashSet<>(this.groupFields);
    for (AggregationLayout agg : this.aggregations) {
      ret.add(agg.getFieldName());
    }
    ret.addAll(this.selection.getSchemaSet());
    return ret;
  }

}
