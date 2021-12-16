package com.nus.cool.core.cohort;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class CohortKey {
    private int cohort;

    private int age;

    private List<String> userlist;

    public CohortKey(int cohort, int age, List<String> userlist) {
        checkArgument(cohort >= 0 && age >= 0);
        this.cohort = cohort;
        this.age = age;
        this.userlist = userlist;
    }

    public CohortKey(int cohort, int age) {
        checkArgument(cohort >= 0 && age >= 0);
        this.cohort = cohort;
        this.age = age;
        this.userlist = null;
    }

    public List<String> getUserlist() {
        return userlist;
    }

    public int getCohort() {
        return cohort;
    }

    public int getAge() {
        return age;
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + cohort;
        result = 31 * result + age;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        boolean bEqual = false;
        if (obj instanceof CohortKey) {
            CohortKey that = (CohortKey) obj;
            return this.cohort == that.cohort &&
                    this.age == that.age;
        }
        return bEqual;
    }

    @Override
    public String toString() {
        return String.format("(c = %d, age = %d)", cohort, age);
    }

}
