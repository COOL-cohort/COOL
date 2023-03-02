package com.nus.cool.core.cohort.cohortselect;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.base.Preconditions;
import com.nus.cool.core.cohort.filter.FilterType;
import com.nus.cool.core.cohort.storage.Scope;
import com.nus.cool.core.field.FloatRangeField;
import java.util.ArrayList;
import lombok.Getter;

/**
 * Mappered from json file into CohortSelectionLayout
 * Parse it to generate different CohortSelection instance.
 */
@Getter
public class CohortSelectionLayout {

  private String fieldSchema;

  private FilterType type = FilterType.ALL;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Float max;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Float min;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Float interval;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String[] acceptValue;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String[] rejectValue;

  /**
   * Create a CohortSelector instance.
   *
   * @return CohortSelector
   * @throws IllegalArgumentException when type is out of scope
   */
  public CohortSelector generate() throws IllegalArgumentException {
    switch (type) {
      case Set:
        return generateSetSelector();
      case Range:
        return generateRangeSelector();
      case ALL:
        return CohortSelector.generateAllSelector(this.fieldSchema);
      default:
        throw new IllegalArgumentException(
            String.format("No filter of this type named %s", type.toString()));
    }
  }

  private CohortRangeSelector generateRangeSelector() {
    Preconditions.checkArgument(this.max != null,
        "Max attribute in RangeCohortSelection should not be NULL");
    Preconditions.checkArgument(this.min != null,
        "Min attribute in SetCohortSelection should not be NULL");
    Preconditions.checkArgument(this.max > this.min, "Max should be larger than Min");
    if (this.interval == null) {
      this.interval = 1.0f;
    }
    // guoyu should use type to differentiate if we use int or float.
    ArrayList<Scope> scopeList = new ArrayList<>();
    for (Float i = this.min; i <= this.max; i += this.interval) {
      Float uplevel = i + this.interval > this.max ? this.max + 1 : i + this.interval;
      Scope u = new Scope(new FloatRangeField(i), new FloatRangeField(uplevel));
      scopeList.add(u);
    }
    return new CohortRangeSelector(this.fieldSchema, scopeList);
  }

  /**
   * If set cohort selector, and no rejectValue and acceptValue input.
   * We consider the situation that all value in this schema can be chosen as a
   */
  private CohortSetSelector generateSetSelector() {
    return new CohortSetSelector(this.fieldSchema, this.acceptValue, this.rejectValue);
  }
}
