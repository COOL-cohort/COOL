package com.nus.cool.extension.storageservice;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import com.nus.cool.storageservice.StorageService;
import com.nus.cool.storageservice.StorageServiceStatus;

import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HdfsStorageServiceIntegrationTest {
  
  // @Test
  public void testSaveLoad() {
    
    java.nio.file.Path dummyDir = java.nio.file.Paths.get("dummy");
    java.nio.file.Path reloadDir = java.nio.file.Paths.get("loaddummy");
    File dummyFile = new File("dummy/dummy2.txt");

    try {
      Files.createDirectories(dummyDir);
      dummyFile.createNewFile();
    } catch (IOException e) {
      Assert.fail("Unable to create dummy files for testing");
    }

    try {
      // replace the host and port with valid input of your hdfs.
      StorageService storageService = new HdfsStorageService("hdfs://<host>:<port>", "/user/hdfs/", new Configuration());
      
      StorageServiceStatus status = storageService.storeData("dummydata", dummyDir); 
      if(status.getCode() != StorageServiceStatus.StatusCode.OK) {
        Assert.fail(status.getMsg());
      };
      status = storageService.loadData("dummydata", reloadDir);
      if(status.getCode() != StorageServiceStatus.StatusCode.OK) {
        Assert.fail(status.getMsg());
      }
    } catch (IOException | URISyntaxException e) {
      Assert.fail("Failed to connect to to hdfs service" + 
        "at local host port 20131\n");
    }
    try {
      dummyFile.delete();
      Files.delete(dummyDir);
      Files.walk(reloadDir)
           .sorted(Comparator.reverseOrder())
           .map(Path::toFile)
           .forEach(File::delete);
      Assert.assertFalse(Files.exists(reloadDir));
      Assert.assertFalse(Files.exists(dummyDir));
    } catch (IOException e) {
      Assert.fail("Failed to garbage clean dummy files for testing" +
        "please remove them manually\n");
    }
  }
}
