package com.nus.cool.queryserver.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.ExtendedCohortQuery;
import com.nus.cool.core.cohort.funnel.FunnelQuery;
import com.nus.cool.queryserver.model.QueryServerModel;
import com.nus.cool.queryserver.utils.Util;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.ws.rs.*;
import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/cohort")
public class CohortController {


    @GetMapping(value = "/list",
            produces = org.springframework.http.MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String[]> listCohorts(@QueryParam("cube") String cube) {
        Util.getTimeClock();
        System.out.println("[*] Server is listing all cohorts."+cube);
        return QueryServerModel.listCohorts(cube);
    }

    @PostMapping(value = "/selection",
            produces = MediaType.APPLICATION_JSON_VALUE,
            consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<List<String>> cohortSelection(@RequestParam("queryFile") MultipartFile queryFile) throws IOException {
        Util.getTimeClock();
        System.out.println("[*] Server is performing the cohort query form IP: ");
        System.out.println("[*] This query is for cohort selection: " + queryFile);
        String queryContent = new String(queryFile.getBytes());
        ObjectMapper mapper = new ObjectMapper();
        ExtendedCohortQuery q = mapper.readValue(queryContent, ExtendedCohortQuery.class);
        return QueryServerModel.cohortSelection(q);
    }

    @PostMapping(value = "/cohort-analysis",
            produces = MediaType.APPLICATION_JSON_VALUE,
            consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<List<String>> performCohortAnalysis(@RequestParam("queryFile") MultipartFile queryFile) throws IOException {
        Util.getTimeClock();
        System.out.println("[*] Server is performing the cohort query form IP: ");
        System.out.println("[*] This query is for cohort analysis: " + queryFile);
        String queryContent = new String(queryFile.getBytes());
        ObjectMapper mapper = new ObjectMapper();
        ExtendedCohortQuery q = mapper.readValue(queryContent, ExtendedCohortQuery.class);
        return QueryServerModel.cohortAnalysis(q);
    }

    @PostMapping(value = "/funnel-analysis",
            produces = MediaType.APPLICATION_JSON_VALUE,
            consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<String> performFunnelAnalysis(@RequestParam("queryFile") MultipartFile queryFile) throws IOException {
        Util.getTimeClock();
        System.out.println("[*] Server is performing the cohort query form IP: ");
        System.out.println("[*] This query is for funnel analysis: " + queryFile);
        String queryContent = new String(queryFile.getBytes());
        ObjectMapper mapper = new ObjectMapper();
        FunnelQuery q = mapper.readValue(queryContent, FunnelQuery.class);
        return QueryServerModel.funnelAnalysis(q);
    }

}
