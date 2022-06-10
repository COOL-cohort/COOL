package com.nus.cool.core.cohort.refactor;

public class TimeUtils {

    public enum TimeUnit {
	
        HOUR("HOUR"),
        
        DAY("DAY"),
        
        WEEK("WEEK"),
        
        MONTH("MONTH");


        private final String text;
        TimeUnit(final String text) {
            this.text = text;
        }

        @Override
        public String toString(){
            return text;
        }
    }


    public class TimeWindow {
        int length;
        TimeUnit unit;
    }
    
}
