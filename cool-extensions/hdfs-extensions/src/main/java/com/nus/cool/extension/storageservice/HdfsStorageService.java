package com.nus.cool.extension.storageservice;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;

import com.nus.cool.storageservice.StorageService;
import com.nus.cool.storageservice.StorageServiceStatus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsStorageService implements StorageService {
  
  private final Path root;

  private FileSystem fs; 

  HdfsStorageService(String rootPath, Configuration conf)
    throws IOException {
    this.root = new Path(rootPath);
    fs = FileSystem.get(conf);
  }

  HdfsStorageService(String hdfsHost, String rootPath, Configuration conf) 
    throws URISyntaxException, IOException {
    this.root = new Path(rootPath);
    fs = FileSystem.get(new URI(hdfsHost), conf);
  }

  @Override
  public StorageServiceStatus loadData(String id,
    java.nio.file.Path destDir) {
    try {
      Path srcDir = root.suffix("/"+id);
      if (!fs.exists(srcDir)) {
        return new StorageServiceStatus(
          StorageServiceStatus.StatusCode.ERROR,
          "Data not found in HDFS at" + srcDir.toString());
        }
      Files.createDirectories(destDir);
      fs.copyToLocalFile(srcDir, 
        new Path(destDir.toFile().getAbsolutePath()));
    } catch (IOException e) {
      e.printStackTrace();
      return new StorageServiceStatus(
        StorageServiceStatus.StatusCode.ERROR, 
        "Failed to load data from HDFS");
    }
    return new StorageServiceStatus(
      StorageServiceStatus.StatusCode.OK, "");
  }

  @Override
  public StorageServiceStatus storeData(String id,
    java.nio.file.Path srcDir) {
    try {
      Path destDir = root.suffix("/"+id);
    fs.copyFromLocalFile(new Path(srcDir.toFile().getAbsolutePath()),
      destDir);
    } catch (IOException e) {
      e.printStackTrace();
      return new StorageServiceStatus(
        StorageServiceStatus.StatusCode.ERROR, 
        "Failed to store data from HDFS");  
    } 
    return new StorageServiceStatus(
      StorageServiceStatus.StatusCode.OK, "");    
  }

  @Override
  public StorageServiceStatus loadCube(String cube,
    java.nio.file.Path destDir) {
    return loadData(cube, destDir);
  }

  @Override
  public StorageServiceStatus storeCube(String cube,
    java.nio.file.Path srcDir) {
    return storeData(cube, srcDir);
  }
}
