/*
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

package com.nus.cool.storageservice;

import lombok.Getter;
import lombok.Setter;

/**
 * Storage service status is returned after interaction with
 * storage service, indicating the operation status and retrieve
 * additional information passed by storage service.
 */
public class StorageServiceStatus {
  /**
   * status code indicates the status of a storage service
   * after an operation. It is used to determine if we can continue
   * use the service.
   */
  public static enum StatusCode {
    /**
     * Storage service is unavailable.
     */
    UNAVAILABLE,
    /**
     * Storage service is ok.
     */
    OK,
    /**
     * Errors occurred in storage service.
     */
    ERROR
  }

  // status of the storage service
  @Getter
  final StatusCode code;

  // auxiliary message from storage service
  @Setter
  @Getter
  String msg;

  /**
   * Construct a storage service status with status code and message.
   *
   * @param code status code
   * @param msg  customized message
   */
  public StorageServiceStatus(StatusCode code, String msg) {
    this.code = code;
    this.msg = msg;
  }

  /**
   * Construct a storage service status with status code and empty message.
   *
   * @param code status code
   */
  public StorageServiceStatus(StatusCode code) {
    this.code = code;
    this.msg = "";
  }

  /**
   * output a string representation of the storage service status
   * including the status code and message assigned.
   */
  @Override
  public String toString() {
    return "Storage service status: " + code
        + (msg.isEmpty() ? "" : msg);
  }
}
