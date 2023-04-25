package com.nus.cool.queryserver.handler;

import com.nus.cool.loader.LoadQuery;
import com.nus.cool.queryserver.model.QueryServerModel;
import com.nus.cool.queryserver.utils.Util;
import java.io.IOException;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;


/**
 * DataLoaderHandler.
 */
@RestController
public class DataLoaderHandler {

  @GetMapping(value = "/info")
  public String getIntroduction() {
    Util.getTimeClock();
    return "This is the backend for the COOL system.\n";
  }

  /**
   * load cube.
   */
  @PostMapping(value = "/load",
      produces = MediaType.APPLICATION_JSON_VALUE,
      consumes = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> load(@RequestBody LoadQuery req) throws IOException {
    System.out.println("[*] This query is for loading a new cube: " + req);

    return QueryServerModel.loadCube(req);
  }

}
