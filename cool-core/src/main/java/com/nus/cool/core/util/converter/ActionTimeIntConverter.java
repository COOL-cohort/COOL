package com.nus.cool.core.util.converter;

/**
 * Interface for converting ActionTime field into int.
 */
public interface ActionTimeIntConverter {
  
  // convert the input action time to time since epoch in seconds
  public int toInt(String v);

}
