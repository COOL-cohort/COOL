/**
 * 
 */
package com.nus.cool.queryserver;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Request;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class QueryServerController implements QueryServerAPI {

	public QueryServerController() {
	}

	@Override
	public QueryResult reload(String name) {
		System.out.println(name);
		return QueryResult.ok();
	}
}
