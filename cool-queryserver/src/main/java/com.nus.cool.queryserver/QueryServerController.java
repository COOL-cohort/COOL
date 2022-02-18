/**
 * 
 */
package com.nus.cool.queryserver;

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
	public String getIntroduction() {
		String text = "This is the backend for the COOL system.\n";
		text += "COOL system is a cohort OLAP system specialized for cohort analysis with extremely low latency.\n";
		text += "Workkable urls: \n";
		text += " - [server]:v1/reload?cube=[cube_name]\n";
		return text;
	}

	@Path("reload")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public QueryResult reload(@QueryParam("cube") String cube){
		System.out.println("[*] Reload the cube: " + cube );
		return QueryResult.ok();
	}
}