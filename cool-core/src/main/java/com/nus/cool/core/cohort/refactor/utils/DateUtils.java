package com.nus.cool.core.cohort.refactor;

import java.util.Calendar;
import java.util.Date;

import com.nus.cool.core.cohort.refactor.TimeUtils.TimeUnit;


public class DateUtils {
    /**
     * Create a Calendar Instance from a unix timestamp
     * @param unixTime
     * @return
     */
    public static Calendar createCalender(long unixTime){
        Calendar is = Calendar.getInstance();
        is.setTime(new Date(unixTime));
        return is;
    }


    /**
     * Get Specific part of the date
     * @param calendar
     * @param unit
     * @return Day, Month, Year, HOUR of Date
     */
    public static int getPart(Calendar calendar, TimeUtils.TimeUnit unit){
        switch(unit){
            case DAY:
                return calendar.get(Calendar.DAY_OF_MONTH);
            case HOUR:
                return calendar.get(Calendar.HOUR_OF_DAY);
            case MONTH:
                return calendar.get(Calendar.MONTH);
            case WEEK:
                return calendar.get(Calendar.WEEK_OF_YEAR);
            case MINUTE:
                return calendar.get(Calendar.MINUTE);
            case SECOND:
                return calendar.get(Calendar.SECOND);
            case YEAR:
                return calendar.get(Calendar.YEAR);
            default:
                throw new IllegalStateException("Illegal TimeUnit");
        }
    }

    public static TimeWindow getDifference(Calendar calendar1, Calendar calendar2, TimeUtils.TimeUnit unit){
        
        long diff;
        // Change to Unix Timestamp, MillSeconds
        if (calendar1.before(calendar2)) {
            diff = calendar2.getTimeInMillis() - calendar1.getTimeInMillis();
        } else {
            diff = calendar1.getTimeInMillis() - calendar2.getTimeInMillis();
        }

        switch(unit){
            case DAY:
                return new TimeWindow(
                    java.util.concurrent.TimeUnit.MILLISECONDS.toDays(diff),
                    TimeUnit.DAY);
            case HOUR:
                return new TimeWindow(
                    java.util.concurrent.TimeUnit.MILLISECONDS.toHours(diff),
                    TimeUnit.HOUR);
            case MINUTE:
                return new TimeWindow(
                    java.util.concurrent.TimeUnit.MILLISECONDS.toMinutes(diff),
                    TimeUnit.MINUTE);
            case MONTH:
                return new TimeWindow(
                    java.util.concurrent.TimeUnit.MILLISECONDS.toDays(diff)/30, 
                    TimeUnit.MONTH);
            case SECOND:
                return new TimeWindow(
                    java.util.concurrent.TimeUnit.MILLISECONDS.toSeconds(diff), 
                    TimeUnit.SECOND);
            case WEEK:
                return new TimeWindow(
                    java.util.concurrent.TimeUnit.MILLISECONDS.toDays(diff)/7, 
                    TimeUnit.WEEK);
            case YEAR:
                return new TimeWindow(
                    java.util.concurrent.TimeUnit.MILLISECONDS.toDays(diff)/365, 
                    TimeUnit.YEAR);
            default:
                throw new IllegalStateException("Illegal TimeUnit");
        }
    }
}
