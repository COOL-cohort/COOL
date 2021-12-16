package com.nus.cool.core.cohort;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
public class QueryResult {

    private QueryStatus status;
    private String errMsg;
    private long elapsed;
    private Object result;

    public QueryResult(QueryStatus status, String errMsg, Object result) {
        this.status = status;
        this.errMsg = errMsg;
        this.result = result;
    }

    public static QueryResult ok(Object result) {
        return new QueryResult(QueryStatus.OK, null, result);
    }

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    public enum QueryStatus {
        OK,

        ERROR
    }
}
