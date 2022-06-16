package com.nus.cool.core.cohort.refactor.cohortSelect;
import com.nus.cool.core.cohort.refactor.filter.FilterType;

public class CohortSelectionLayout {

    private String fieldSchema;

    private FilterType type;

    private Integer max, min, interval;

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