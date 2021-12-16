/*
 * Copyright 2004 Sean Owen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rabinhash;

/**
 * <p>A collection of utility methods used throughout this package.</p>
 *
 * @author Sean Owen
 * @version 2.0
 * @since 2.0
 */
final class RabinHashFunctionUtils {

  private static final int BIT_32 = 0x80000000;
  private static final long BIT_64 = 0x8000000000000000L;

  /**
   * Private constructor.
   */
  private RabinHashFunctionUtils() {
  }

  static String polynomialToString(final int P) {
    final StringBuffer result = new StringBuffer();
    result.append("x^32");
    int exp = 31;
    for (int temp = P; temp != 0; temp <<= 1, exp--) {
      if ((temp & BIT_32) != 0) {
        appendTerm(exp, result);
      }
    }
    return result.toString();
  }

  static String polynomialToString(final long P) {
    final StringBuffer result = new StringBuffer();
    result.append("x^64");
    int exp = 63;
    for (long temp = P; temp != 0; temp <<= 1, exp--) {
      if ((temp & BIT_64) != 0) {
        appendTerm(exp, result);
      }
    }
    return result.toString();
  }

  private static void appendTerm(final int exp, final StringBuffer result) {
    switch (exp) {
      case 0:
        result.append(" + 1");
        break;
      case 1:
        result.append(" + x");
        break;
      default:
        result.append(" + x^");
        result.append(exp);
        break;
    }
  }
}
