package com.nus.cool.queryserver;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;


@Path("/v1")
public interface QueryServerAPI {
	@Path("reload")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	QueryResult reload(@QueryParam("name") String name);

}
