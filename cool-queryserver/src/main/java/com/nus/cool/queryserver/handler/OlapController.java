package com.nus.cool.queryserver.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.OLAPQueryLayout;
import com.nus.cool.core.iceberg.query.IcebergQuery;
import com.nus.cool.queryserver.model.QueryServerModel;
import com.nus.cool.queryserver.utils.Util;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;


@RestController
@RequestMapping("/olap")
public class OlapController {

  @PostMapping(value = "/olap",
      produces = MediaType.APPLICATION_JSON_VALUE,
      consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  public ResponseEntity<String> performOLAPQuery(
      @RequestParam("queryFile") MultipartFile queryFile) throws IOException {
    System.out.println("[*] Server serve OLAP query, " + queryFile);
    String queryContent = new String(queryFile.getBytes());
    ObjectMapper mapper = new ObjectMapper();
    OLAPQueryLayout q = mapper.readValue(queryContent, OLAPQueryLayout.class);
    return QueryServerModel.precessIcebergQuery(q);
  }

}
