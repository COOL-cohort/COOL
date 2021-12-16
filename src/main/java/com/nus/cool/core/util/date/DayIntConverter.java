package com.nus.cool.core.util.date;

import org.joda.time.DateTime;
import org.joda.time.Days;

/**
 * Convert the input day represented in yyyy-MM-dd to integer
 * which is the number of days past the reference day
 */
public class DayIntConverter {

    /**
     * Convert date string value to the number of days
     * past the reference day.
     *
     * @param v date string value
     * @return number of days past the reference day
     */
    public static int toInt(String v) {
        DateTime end = DateBase.FORMATTER.parseDateTime(v);
        return Days.daysBetween(DateBase.BASE, end).getDays();
    }

    /**
     * Get date according to number of days past the
     * reference day.
     *
     * @param days number of days past the reference day
     * @return date string value for specific format
     */
    public static String getString(int days) {
        DateTime dt = DateBase.BASE.plusDays(days);
        return DateBase.FORMATTER.print(dt);
    }
}
