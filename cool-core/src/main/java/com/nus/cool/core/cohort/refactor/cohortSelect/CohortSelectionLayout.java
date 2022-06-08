package com.nus.cool.core.cohort.refactor.cohortSelect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.nus.cool.core.cohort.refactor.filter.FilterType;

import lombok.Getter;

@Getter
public class CohortSelectionLayout {

    private String fieldSchema;

    private FilterType type;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Integer max, min, interval;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String[] acceptValue, rejectValue;

    public CohortSelector generateCohortSelector() {
        switch (type) {
            case Set:
                // TODO(lingze): add some pre argument validation
                return new CohortSetSelector(fieldSchema, acceptValue, rejectValue);
            case Range:
                // TODO(lingze): add some pre argument validation
                return CohortRangeSelector.generateCohortRangeSelector(fieldSchema, max, min, interval);
            default:
                throw new IllegalArgumentException(
                        String.format("No filter of this type named %s", type.toString()));

        }
    }
}