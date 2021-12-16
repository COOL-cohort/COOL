/*
 * Copyright 2020 Cool Squad Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nus.cool.core.cohort;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;

/**
 * @author zhongle, hongbin
 * @version 0.1
 * @since 0.1
 */
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
