package com.nus.cool.storageservice;

import lombok.Getter;
import lombok.Setter;

public class StorageServiceStatus {
  public static enum StatusCode {
    UNAVAILABLE,
    OK,
    ERROR
  }

  // status of the storage service
  @Getter
  final StatusCode code;
  // auxiliary message from storage service
  @Setter
  @Getter
  String msg;

  public StorageServiceStatus(StatusCode code, String msg) {
    this.code = code;
    this.msg = msg;
  }

  public StorageServiceStatus(StatusCode code) {
    this.code = code;
    this.msg = "";
  }

  @Override
  public String toString() {
    return "Storage service status: " + code 
      + (msg.isEmpty() ? "" : msg);  
  }
}
