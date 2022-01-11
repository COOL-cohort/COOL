/*
 * Copyright 2021 Cool Squad Team
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.nus.cool.core.cohort;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;

/**
 * CohortKey defines the characteristic of a cohort
 */
public class CohortKey {

 /**
 * cohort ID
 */
  private int cohort;

  /**
   * The age of the cohort 
   */
  private int age;

  /**
   * The users in the cohort
   */
  private List<String> userlist;

  /**
   * construction function of the class
   * 
   * @param cohort the cohort id of the target cohort
   * @param age the age of the target cohort
   * @param userlist the Userlist of the target cohort
   */
  public CohortKey(int cohort, int age, List<String> userlist) {
    checkArgument(cohort >= 0 && age >= 0);
    this.cohort = cohort;
    this.age = age;
    this.userlist = userlist;
  }

  /**
   * construction function of the class
   * 
   * @param cohort the cohort id of the target cohort
   * @param age the age of the target cohort
   */
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

  /**
   * The hash code of the cohort, it could be used to identify cohorts
   */
  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + cohort;
    result = 31 * result + age;
    return result;
  }

  /**
   * Whether the target CohortKey class equals this object
   * 
   * @return 0 inidcates they are different and 1 indicates they are the same
   */
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

  /**
   * Get the string of cohort id and age
   */
  @Override
  public String toString() {
    return String.format("(c = %d, age = %d)", cohort, age);
  }

}
