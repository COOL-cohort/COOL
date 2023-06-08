package com.nus.cool.queryserver.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.OLAPProcessor;
import com.nus.cool.core.cohort.OLAPQueryLayout;
import com.nus.cool.core.cohort.storage.OLAPRet;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.queryserver.singleton.ModelConfig;
import java.io.IOException;
import java.util.List;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;


/**
 * OlapController.
 */
@RestController
@RequestMapping("/olap")
public class OlapController {

  /**
   * performOLAPQuery controller.
   */
  @PostMapping(value = "/olap",
      produces = MediaType.APPLICATION_JSON_VALUE,
      consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  public ResponseEntity<String> performOLAPQuery(
      @RequestParam("queryFile") MultipartFile queryFile) throws IOException {
    System.out.println("[*] Server serve OLAP query, " + queryFile);
    String queryContent = new String(queryFile.getBytes());
    ObjectMapper mapper = new ObjectMapper();
    OLAPQueryLayout layout = mapper.readValue(queryContent, OLAPQueryLayout.class);
    String inputSource = layout.getDataSource();
    ModelConfig.cachedCoolModel.reload(inputSource);
    OLAPProcessor olapProcessor = new OLAPProcessor(layout);
    // start a new cool model and reload the cube
    CubeRS cube = ModelConfig.cachedCoolModel.getCube(layout.getDataSource());
    List<OLAPRet> ret = olapProcessor.processCube(cube);
    return ResponseEntity.ok().body(ret.toString());
  }

}
