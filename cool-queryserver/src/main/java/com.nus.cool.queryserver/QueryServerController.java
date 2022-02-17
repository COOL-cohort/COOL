/**
 * 
 */
package com.nus.cool.queryserver;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Request;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MediaType;

@Path("v1")
public class QueryServerController {

	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String getM() {
		return "My message\n";
	}

	@Path("msg")
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String getMessage() {
		return "My message\n";
	}

	@Path("name")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String name(@QueryParam("name") String name){
		System.out.println(name);
		return name;
	}
}