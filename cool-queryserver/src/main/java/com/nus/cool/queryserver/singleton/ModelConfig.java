package com.nus.cool.queryserver.singleton;

import com.nus.cool.model.CoolModel;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import lombok.Data;

/**
 * Global config.
 */
@Data
public class ModelConfig {

  public static String dataSourcePath;

  public static Properties props;

  public static CoolModel cachedCoolModel;

  private static volatile ModelConfig instance = null;

  /**
   * getInstance.
   */
  public static ModelConfig getInstance() throws IOException {
    if (instance == null) {
      synchronized (ModelConfig.class) {
        if (instance == null) {
          instance = new ModelConfig();
        }
      }
    }
    return instance;
  }

  private ModelConfig() {
    props = new Properties();
    try {
      props.load(new FileInputStream(
          "./application.properties"));
      dataSourcePath = props.getProperty("datasource.path");
      cachedCoolModel = new CoolModel(dataSourcePath);

    } catch (IOException e) {
      System.err.println("Error loading config file: " + e.getMessage());
      System.exit(1);
    }
  }
}
