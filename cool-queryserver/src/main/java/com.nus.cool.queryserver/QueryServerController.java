/**
 * 
 */
package com.nus.cool.queryserver;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.util.Date;
import java.io.IOException;

import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.funnel.FunnelQuery;
import com.nus.cool.core.iceberg.query.IcebergQuery;
import org.eclipse.jetty.server.Request;

import com.nus.cool.core.cohort.ExtendedCohortQuery;

@Path("v1")
public class QueryServerController {
	private QueryServerModel qsModel;

	public QueryServerController(QueryServerModel model){
		this.qsModel = model;
	}

	private void getTimeClock(){
		System.out.println("======================== " + new Date() + " ========================");
	}

	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String getIntroduction() {
		getTimeClock();
		String text = "This is the backend for the COOL system.\n";
		text += "COOL system is a cohort OLAP system specialized for cohort analysis with extremely low latency.\n";
		text += "Workkable urls: \n";
		text += "HTTP Method: GET\n";
		text += " - [server]:v1\n";
		text += " - [server]:v1/reload?cube=[cube_name]\n";
		text += " - [server]:v1/list\n";
		text += " - [server]:v1/cohort/list?cube=[cube_name]\n";
		text += "HTTP Method: POST\n";
		text += " - [server]:v1/cohort/selection\n";
		text += " - [server]:v1/cohort/analysis\n";
		text += " - [server]:v1/funnel/analysis\n";
		return text;
	}

	@Path("reload")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response reload(@QueryParam("cube") String cube) throws IOException {
		getTimeClock();
		System.out.println("[*] Server is reloading the cube: " + cube );
		Response res = qsModel.reloadCube(cube);
		return res;
	}

	@Path("list")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response listCubes() {
		getTimeClock();
		System.out.println("[*] Server is listing all cubes.");
		Response res = qsModel.listCubes();
		return res;
	}

	@Path("cohort/list")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response listCohorts(@QueryParam("cube") String cube) {
		getTimeClock();
		System.out.println("[*] Server is listing all cohorts.");
		Response res = qsModel.listCohorts(cube);
		return res;
	}

	@Path("cohort/selection")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.TEXT_PLAIN)
	public Response cohortSelection(@Context Request req, String query) throws IOException {
		getTimeClock();
		System.out.println("[*] Server is performing the cohort query form IP: " + req.getRemoteAddr());
		System.out.println("[*] This query is for cohort selection: " + query);
		ObjectMapper mapper = new ObjectMapper();
		ExtendedCohortQuery q = mapper.readValue(query, ExtendedCohortQuery.class);
		Response res = qsModel.cohortSelection(q);
		return res;
	}

	@Path("cohort/exploration")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.TEXT_PLAIN)
	public Response cohortExploration(@Context Request req, String cube, String cohort) throws IOException {
		getTimeClock();
		System.out.println("[*] Server is performing the cohort query form IP: " + req.getRemoteAddr());
		System.out.println(String.format("[*] This query is for cohort exploration: %s %s", cube, cohort));
		Response res = qsModel.cohortExploration(cube, cohort);
		return res;
	}

	@Path("cohort/analysis")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.TEXT_PLAIN)
	public Response performCohortAnalysis(@Context Request req, String query) throws IOException {
		getTimeClock();
		System.out.println("[*] Server is performing the cohort query form IP: " + req.getRemoteAddr());
		System.out.println("[*] This query is for cohort analysis: " + query);
		ObjectMapper mapper = new ObjectMapper();
		ExtendedCohortQuery q = mapper.readValue(query, ExtendedCohortQuery.class);
		Response res = qsModel.cohortAnalysis(q);
		return res;
	}

	@Path("funnel/analysis")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.TEXT_PLAIN)
	public Response performFunnelAnalysis(@Context Request req, String query) throws IOException {
		getTimeClock();
		System.out.println("[*] Server is performing the cohort query form IP: " + req.getRemoteAddr());
		System.out.println("[*] This query is for funnel analysis: " + query);
		ObjectMapper mapper = new ObjectMapper();
		FunnelQuery q = mapper.readValue(query, FunnelQuery.class);
		Response res = qsModel.funnelAnalysis(q);
		return res;
	}

	@Path("olap/iceberg")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.TEXT_PLAIN)
	public Response performIcebergQuery(@Context Request req, String query) throws IOException {
		getTimeClock();
		System.out.println("[*] Server is performing the cohort query form IP: " + req.getRemoteAddr());
		System.out.println("[*] This query is for funnel analysis: " + query);
		ObjectMapper mapper = new ObjectMapper();
		IcebergQuery q = mapper.readValue(query, IcebergQuery.class);
		Response res = qsModel.precessIcebergQuery(q);
		return res;
	}
}