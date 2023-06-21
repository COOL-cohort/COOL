package com.nus.cool.queryserver.handler;

import com.nus.cool.core.util.config.CsvDataLoaderConfig;
import com.nus.cool.core.util.config.DataLoaderConfig;
import com.nus.cool.extension.util.config.AvroDataLoaderConfig;
import com.nus.cool.extension.util.config.ParquetDataLoaderConfig;
import com.nus.cool.model.CoolLoader;
import com.nus.cool.queryserver.model.LoadQuery;
import com.nus.cool.queryserver.utils.Util;
import java.io.File;
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
  public ResponseEntity<String> load(@RequestBody LoadQuery query) {
    System.out.println("[*] This query is for loading a new cube: " + query);
    try {
      query.isValid();
      String fileType = query.getDataFileType().toUpperCase();
      DataLoaderConfig config;
      switch (fileType) {
        case "CSV":
          config = new CsvDataLoaderConfig();
          break;
        case "PARQUET":
          config = new ParquetDataLoaderConfig();
          break;
        case "AVRO":
          config = new AvroDataLoaderConfig(new File(query.getConfigPath()));
          break;
        default:
          throw new IllegalArgumentException("[x] Invalid load file type: " + fileType);
      }
      System.out.println(config.getClass().getName());
      CoolLoader coolLoader = new CoolLoader(config);
      String out = coolLoader.load(query.getCubeName(), query.getSchemaPath(), query.getDataPath(),
          query.getOutputPath());
      return ResponseEntity.ok().body(out);
    } catch (Exception e) {
      return ResponseEntity.internalServerError().body(e.toString());
    }
    // String resStr = "Cube " + query.getCubeName() + " is loaded successfully";
  }

}
