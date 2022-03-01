package com.nus.cool.queryserver;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author david
 *
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class QueryResult {
	
	public static enum QueryStatus { OK, ERROR }
	
	private QueryStatus status;
	
	private String errMsg;
	
	private long elapsed;
	
	private Object result;
	
	public QueryResult() {
		
	}
	
	public static QueryResult ok() {
		return new QueryResult(QueryStatus.OK, null, null);
	}
	
	public static QueryResult ok(Object result) {
		return new QueryResult(QueryStatus.OK, null, result);
	}
	
	public static QueryResult error(String msg) {
		return new QueryResult(QueryStatus.ERROR, msg, null);
	}
	
	public QueryResult(QueryStatus status, String errMsg, Object result) {
		this.status = status;
		this.setErrMsg(errMsg);
		this.result = result;
	}

	/**
	 * @return the result
	 */
	public Object getResult() {
		return result;
	}

	/**
	 * @param result the result to set
	 */
	public void setResult(Object result) {
		this.result = result;
	}


	/**
	 * @return the errMsg
	 */
	public String getErrMsg() {
		return errMsg;
	}

	/**
	 * @param errMsg the errMsg to set
	 */
	public void setErrMsg(String errMsg) {
		this.errMsg = errMsg;
	}

	/**
	 * @return the status
	 */
	public QueryStatus getStatus() {
		return status;
	}

	/**
	 * @param status the status to set
	 */
	public void setStatus(QueryStatus status) {
		this.status = status;
	}
	
	/**
	 * @return the elapsed
	 */
	public long getElapsedInMS() {
		return elapsed;
	}

	/**
	 * @param elapsed the elapsed to set
	 */
	public void setElapsedInMS(long elapsed) {
		this.elapsed = elapsed;
	}
	

	@Override
	public String toString() {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			throw new IllegalStateException(e);
		}
	}
	
}
