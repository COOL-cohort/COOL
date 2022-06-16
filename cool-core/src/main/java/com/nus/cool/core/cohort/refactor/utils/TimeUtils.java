package com.nus.cool.core.cohort.refactor;

public class TimeUtils {

    public enum TimeUnit {
	
        HOUR("HOUR"),
        
        DAY("DAY"),
        
        WEEK("WEEK"),
        
        MONTH("MONTH"),

        MINUTE("MINUTE"),

        SECOND("SECOND"),

        YEAR("YEAR");

        

        private final String text;
        TimeUnit(final String text) {
            this.text = text;
        }

        @Override
        public String toString(){
            return text;
        }
    }
    
}
