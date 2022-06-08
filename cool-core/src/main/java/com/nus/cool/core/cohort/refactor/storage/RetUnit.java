package com.nus.cool.core.cohort.refactor.storage;

import lombok.Data;

/**
 * RetUnit to bind value and count.
 * Provide get and set interface
 */
@Data
public class RetUnit{
    // intermediate value for cohort analysis
    private float value;

    // how many action tuple belong to this selection
    // (used to calculate AVERAGE and COUNT)
    private int count;

    public RetUnit(float value, int count){
        this.value = value;
        this.count = count;
    }
}
