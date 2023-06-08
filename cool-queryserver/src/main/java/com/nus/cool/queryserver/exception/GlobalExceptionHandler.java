package com.nus.cool.queryserver.exception;


import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

/**
 * GlobalExceptionHandler.
 */
@ControllerAdvice
public class GlobalExceptionHandler {

  /**
   * handleException.
   */
  @ExceptionHandler(Exception.class)
  public ResponseEntity<String> handleException(Exception e) {

    System.out.println("[*] Server serve OLAP query, " + e.getMessage());

    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body("An error occurred: " + e.getMessage());
  }
}