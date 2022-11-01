package com.nus.cool.core.cohort.refactor.ageselect;

import com.google.common.base.Preconditions;
import com.nus.cool.core.cohort.refactor.storage.Scope;
import com.nus.cool.core.cohort.refactor.utils.TimeUtils;

import lombok.Getter;

@Getter
public class AgeSelectionLayout {
    private TimeUtils.TimeUnit unit;

    private Integer min;

    private Integer max;

    private int interval = 1;

    public AgeSelection generate() throws IllegalArgumentException {
        Preconditions.checkArgument(this.min != null, "AgeSelection's min is not allowed to be missing");
        Preconditions.checkArgument(this.max != null, "AgeSelection's max is not allowed to be missing");
        Scope scope = new Scope(min, max + 1);
        return new AgeSelection(scope, unit);
    }
}
