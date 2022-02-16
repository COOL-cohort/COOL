package com.nus.cool.queryserver;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;


@Path("/")
public class QueryServer {
    @Path("hello")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    QueryResult reload(@QueryParam("name") String name){
        return
    }

}