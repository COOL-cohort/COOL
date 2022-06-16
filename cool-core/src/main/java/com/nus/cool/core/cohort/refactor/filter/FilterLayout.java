package com.nus.cool.core.cohort.refactor.filter;

import com.google.common.base.Preconditions;
import lombok.Getter;

/** Read from json file into FilterLayout 
 *  Parse to construct certain filter class
 **/
public class FilterLayout {
    @Getter
    private String fieldSchema;
    
    @Getter
    private String type;
    
    @Getter
    private String[] acceptValue;

    @Getter
    private String[] rejectValue;

    @Override
    public String toString() {
        return String.format("FilterLayout fieldSchema %s filter type %s, acceptValue %s, rejectValue %s", 
            fieldSchema, type, acceptValue.toString(), rejectValue.toString());
    }


    public Filter generateFilter(){
        if(type == FilterType.Range.toString()) {
            // For RangeFilter, rejectValue property is null 
            Preconditions.checkArgument(rejectValue == null, 
                "For RangeFilter, rejectValue property is null");
            return new RangeFilter(fieldSchema, acceptValue);
        } else if (type == FilterType.Set.toString()){
            return new SetFilter(fieldSchema,acceptValue, rejectValue);
        } else throw new IllegalArgumentException(
                String.format("No filter of this type named %s", type));   
    }
}
