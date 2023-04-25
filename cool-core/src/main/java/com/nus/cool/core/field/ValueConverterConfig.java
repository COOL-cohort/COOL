package com.nus.cool.core.field;

import com.nus.cool.core.util.converter.ActionTimeIntConverter;
import com.nus.cool.core.util.converter.DayIntConverter;
import lombok.Data;


/**
 * Configuration for value converter.
 *  now it contains only how action time is represented as int.
 */
@Data
public class ValueConverterConfig {
  ActionTimeIntConverter actionTimeIntConverter = DayIntConverter.getInstance();
}
