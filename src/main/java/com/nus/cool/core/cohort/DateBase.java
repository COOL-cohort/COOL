/**
 * 
 */
package com.nus.cool.core.cohort;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * The base date for all date-related storage
 * 
 * @author david
 *
 */
public class DateBase {

	public static final DateTimeFormatter FORMATTER;
	
	public static final DateTime BASE;

	static {
		FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
		BASE = FORMATTER.parseDateTime("2013-01-01");
	}
	
}
