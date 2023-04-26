package com.nus.cool.queryserver.model;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.File;
import java.io.IOException;
import lombok.Data;


/**
 * General query context.
 */
@ApiModel("Load cube query object")
@Data
public class LoadQuery {
  @ApiModelProperty("Data file type, e,g, CVS")
  private String dataFileType;
  @ApiModelProperty("Cube Name, e,g, health")
  private String cubeName;
  @ApiModelProperty("Path of schema.yaml")
  private String schemaPath;
  @ApiModelProperty("Path of data.scv")
  private String dataPath;
  @ApiModelProperty("output path for cube and cubelets.")
  private String outputPath;
  private String configPath;

  /**
   * Check query validity.
   */
  public boolean isValid() throws IOException {
    boolean f = true;
    if (dataFileType == "AVRO") {
      f = isExist(configPath);
    }
    return f && isExist(schemaPath) && isExist(dataPath) && cubeName.isEmpty()
      && outputPath.isEmpty();
  }

  private boolean isExist(String path) throws IOException {
    File f = new File(path);
    if (!f.exists()) {
      throw new IOException("[x] File " + path + " does not exist.");
    }
    return true;
  }
}
