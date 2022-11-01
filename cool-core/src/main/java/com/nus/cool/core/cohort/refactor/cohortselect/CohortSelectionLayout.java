package com.nus.cool.core.cohort.refactor.cohortselect;

import java.util.ArrayList;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.base.Preconditions;
import com.nus.cool.core.cohort.refactor.filter.FilterType;
import com.nus.cool.core.cohort.refactor.storage.Scope;

import lombok.Getter;

/**
 * Mappered from json file into CohortSelectionLayout
 * Parse it to generate different CohortSelection instance.
 */
@Getter
public class CohortSelectionLayout {

    private String fieldSchema;

    private FilterType type;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Integer max, min, interval;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String[] acceptValue, rejectValue;

    public CohortSelector generate() throws IllegalArgumentException {
        switch (type) {
            case Set:
                return generateSetSelector();
            case Range:
                return generateRangeSelector();
            default:
                throw new IllegalArgumentException(
                        String.format("No filter of this type named %s", type.toString()));
        }
    }

    private CohortRangeSelector generateRangeSelector() {
        Preconditions.checkArgument(this.max != null, "Max attribute in RangeCohortSelection should not be NULL");
        Preconditions.checkArgument(this.min != null, "Min attribute in SetCohortSelection should not be NULL");
        Preconditions.checkArgument(this.max > this.min, "Max should be larger than Min");
        if (this.interval == null) {
            this.interval = 1;
        }
        ArrayList<Scope> scopeList = new ArrayList<>();
        for (int i = this.min; i <= this.max; i += this.interval) {
            int uplevel = i + this.interval > this.max ? this.max + 1 : i + this.interval;
            Scope u = new Scope(i, uplevel);
            scopeList.add(u);
        }
        return new CohortRangeSelector(this.fieldSchema, scopeList);
    }

    private CohortSetSelector generateSetSelector() {
        /**
         * If set cohort selector, and no rejectValue and acceptValue input.
         * We consider the situation that all value in this schema can be chosen as a
         */
        if (this.acceptValue == null && this.rejectValue == null) {
            this.rejectValue = new String[0];
        }
        return new CohortSetSelector(this.fieldSchema, this.acceptValue, this.rejectValue);
    }

}
