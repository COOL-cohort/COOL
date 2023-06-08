package com.nus.cool.queryserver.response;

import lombok.Data;

/**
 * This is the ResponseWrapper.
 */
@Data
public class ResponseWrapper<T> {

  private String message;
  private T data;
}
