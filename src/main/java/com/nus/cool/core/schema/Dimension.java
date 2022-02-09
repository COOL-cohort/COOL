/**
 * 
 */
package com.nus.cool.core.schema;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * @author david
 *
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Dimension {
	
	public static enum DimensionType {
		
		NORMAL,
		
		PROPERTY,
		
		CALC
		
	}
	
	private DimensionType type;
	
	private String name;
	
	private String tableFieldName;
	
	private List<String> values;
	
	private FieldType fieldType;

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

	/**
	 * @return the type
	 */
	public DimensionType getDimensionType() {
		return type;
	}

	/**
	 * @param type the type to set
	 */
	public void setDimensionType(DimensionType type) {
		this.type = type;
	}

	/**
	 * @return the values
	 */
	public List<String> getValues() {
		return values;
	}

	/**
	 * @param values the values to set
	 */
	public void setValues(List<String> values) {
		this.values = values;
	}

	/**
	 * @return the fieldType
	 */
	public FieldType getTableFieldType() {
		return fieldType;
	}

	/**
	 * @param fieldType the fieldType to set
	 */
	public void setTableFieldType(FieldType fieldType) {
		this.fieldType = fieldType;
	}
	
}
