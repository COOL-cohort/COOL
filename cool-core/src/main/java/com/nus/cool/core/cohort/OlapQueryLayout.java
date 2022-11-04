package com.nus.cool.core.cohort.refactor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.refactor.olapSelect.Aggregation;
import com.nus.cool.core.cohort.refactor.olapSelect.olapSelectionLayout;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import lombok.Getter;
import java.io.File;


@Getter
public class OlapQueryLayout {

  // inner structure
  public enum granularityType{
    DAY,
    MONTH,
    YEAR,
    NULL
  }

  // datasource name
  @JsonProperty("dataSource")
  private String dataSource;

  // select condition
  @JsonProperty("selection")
  private olapSelectionLayout selection;

  // a list a groupFields
  @JsonProperty("groupFields")
  private List<String> groupFields;

  // a list of aggregation functions, sum, count etc
  @JsonProperty("aggregations")
  private List<Aggregation> aggregations;

  // selected time range
  @JsonProperty("timeRange")
  private String timeRange;

  // granularity for time range
  @JsonProperty("granularity")
  private granularityType granularity;

  // granularity for groupBy, if the groupBy field is dataType,
  @JsonProperty("groupFields_granularity")
  private granularityType groupFields_granularity;

  public static OlapQueryLayout readFromJson(File inputFile) throws IOException{
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(inputFile, OlapQueryLayout.class);
  }

  public static OlapQueryLayout readFromJson(String path) throws IOException{
    return readFromJson(new File(path));
  }

  /**
   * Return the schema set.
   */
  public HashSet<String> getSchemaSet() {

    HashSet<String> ret = new HashSet<>(this.groupFields);
    for (Aggregation agg: this.aggregations){
      ret.add(agg.getFieldName());
    }
    ret.addAll(this.selection.getSchemaSet());
    return ret;
  }

}
