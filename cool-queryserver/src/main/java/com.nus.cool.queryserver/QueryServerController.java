/**
 * 
 */
package com.nus.cool.queryserver;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.IOException;

import javax.ws.rs.core.MediaType;
import org.eclipse.jetty.server.Request;

import com.nus.cool.core.cohort.ExtendedCohortQuery;

@Path("v1")
public class QueryServerController {
	private QueryServerModel qsModel;

	public QueryServerController(QueryServerModel model){
		this.qsModel = model;
	}


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
	public Response reload(@QueryParam("cube") String cube) throws IOException {
		System.out.println("[*] Server is reloading the cube: " + cube );
		Response res = qsModel.reloadCube(cube);
		return res;
	}

	@Path("cohort/create")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response creatCohort(@Context Request req, ExtendedCohortQuery q) throws IOException {
		System.out.println("[*] Server is performing the cohort query form IP: " + req.getRemoteAddr());
		System.out.println("[*] This cohort query is for creating cohorts: " + q);
		Response res = qsModel.creatCohort(q);
		return res;
	}

	@Path("cohort/analysis")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response performCohortAnalysis(@Context Request req, ExtendedCohortQuery q) throws IOException {
		System.out.println("[*] Server is performing the cohort query form IP: " + req.getRemoteAddr());
		System.out.println("[*] This cohort query is for cohort analysis: " + q);
		Response res = qsModel.cohrtAnalysis(q);
		return res;
	}
}