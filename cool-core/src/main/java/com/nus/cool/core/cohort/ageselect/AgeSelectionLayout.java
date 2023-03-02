package com.nus.cool.core.cohort.ageselect;

import com.google.common.base.Preconditions;
import com.nus.cool.core.cohort.storage.Scope;
import com.nus.cool.core.cohort.utils.TimeUtils;
import com.nus.cool.core.field.IntRangeField;
import lombok.Getter;

/**
 * AgeSelection Query Layout.
 * Mapped from json file
 */
@Getter
public class AgeSelectionLayout {
  private TimeUtils.TimeUnit unit;

  private Integer min;

  private Integer max;

  private int interval = 1;

  /**
   * Create a ageSelection instance.
   *
   * @return AgeSelection instance
   * @throws IllegalArgumentException Illegal Args breakup constrain
   */
  public AgeSelection generate() throws IllegalArgumentException {
    Preconditions.checkArgument(this.min != null,
        "AgeSelection's min is not allowed to be missing");
    Preconditions.checkArgument(this.max != null,
        "AgeSelection's max is not allowed to be missing");
    Scope scope = new Scope(new IntRangeField(min), new IntRangeField(max + 1));
    return new AgeSelection(scope, unit);
  }
}
