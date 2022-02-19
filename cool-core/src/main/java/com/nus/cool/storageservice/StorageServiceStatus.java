package com.nus.cool.storageservice;

import lombok.Getter;
import lombok.Setter;

/**
 * Storage service status is returned after interaction with
 *  storage service, indicating the operation status and retrieve
 *  additional information passed by storage service
 */
public class StorageServiceStatus {
  /**
   * status code indicates the status of a storage service
   *  after an operation. It is used to determine if we can continue
   *  use the service.
   */
  public static enum StatusCode {
    /**
     * Storage service is unavaible
     */
    UNAVAILABLE,
    /**
     * Storage service is ok
     */
    OK,
    /**
     * Errors occured in storage service
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
   * Construct a storage service status with status code and message
   * 
   * @param code status code
   * @param msg customized message
   */
  public StorageServiceStatus(StatusCode code, String msg) {
    this.code = code;
    this.msg = msg;
  }

  /**
   * Construct a storge service status with status code and empty message
   * 
   * @param code status code
   */
  public StorageServiceStatus(StatusCode code) {
    this.code = code;
    this.msg = "";
  }

  /**
   * output a string representation of the storage service status
   *  including the status code and message assigned.
   */
  @Override
  public String toString() {
    return "Storage service status: " + code 
      + (msg.isEmpty() ? "" : msg);  
  }
}
