/**
 * 
 */
package com.nus.cool.core.schema;

/**
 * @author david
 *
 */
public class Measure {
	
	public static enum MeasureType {
		
		ROLLRETENTION,

        RETENTION,
		
		COUNT,
		
		SUM,

        MAX,

        MIN,

        AVG
		
	}
	
	private MeasureType aggregator;
	
	private String name;
	
	private String tableFieldName;

    public Measure(String aggregator, String name, String field) {
        this.name = name;
        this.tableFieldName = field;
        this.aggregator = MeasureType.RETENTION;
        if (aggregator.equals(MeasureType.COUNT.name())) {
            this.aggregator = MeasureType.COUNT;
        } else if (aggregator.equals(MeasureType.SUM.name())) {
            this.aggregator = MeasureType.SUM;
        } else if (aggregator.equals(MeasureType.RETENTION.name())) {
            this.aggregator = MeasureType.RETENTION;
        } 
    }
	
	public Measure() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @return the aggregator
	 */
	public MeasureType getAggregator() {
		return aggregator;
	}

	/**
	 * @param aggregator the aggregator to set
	 */
	public void setAggregator(MeasureType aggregator) {
		this.aggregator = aggregator;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the tableFieldName
	 */
	public String getTableFieldName() {
		return tableFieldName;
	}

	/**
	 * @param tableFieldName the tableFieldName to set
	 */
	public void setTableFieldName(String tableFieldName) {
		this.tableFieldName = tableFieldName;
	}
	
}
