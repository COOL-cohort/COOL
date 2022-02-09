package com.nus.cool.core.cohort;

import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.util.converter.DateBase;
import org.joda.time.DateTime;
import org.joda.time.Days;

import static com.google.common.base.Preconditions.checkArgument;


public class TimeUtils {
	/**
	 * @brief skip to the offset which corresponds to the first activity after
	 *        the given date
	 *
	 * @param fromOffset
	 * @param endOffset
	 *
	 * @return the offset of the first activity; or endOffset if not found
	 */
	public static int skipToDate(InputVector vector, int fromOffset, int endOffset, int date) {
		//checkArgument(fromOffset <= endOffset);
		vector.skipTo(fromOffset);
		while (fromOffset < endOffset) {
			if (getDate(vector.next()) >= date) {
				break;
			}
			++fromOffset;
		}
		return fromOffset;
	}
	
	public static int getDate(int days) {
		return days;
	}

    public static int getDateofNextTimeUnitN(int startDate, TimeUnit unit, int nextN) {
        checkArgument(nextN >= 0);
        switch(unit) {
            case DAY:
                return startDate + nextN;
            case WEEK:
                DateTime now = DateBase.BASE.plusDays(startDate);
                return Days.daysBetween(DateBase.BASE,
                        now.plusWeeks(nextN).withDayOfWeek(1)).getDays();
            case MONTH:
                now = DateBase.BASE.plusDays(startDate);
                return Days.daysBetween(DateBase.BASE,
                        now.plusMonths(nextN).withDayOfMonth(1)).getDays();
            default:
                throw new IllegalArgumentException("Unkownn time unit: " + unit);
        }
    }
	
	public static int skipToNextTimeUnitN(InputVector vector, TimeUnit unit, int fromOffset, int endOffset, int startDate, int nextN) {
        return skipToDate(vector, fromOffset, endOffset, 
                getDateofNextTimeUnitN(startDate, unit, nextN));
    }
}
