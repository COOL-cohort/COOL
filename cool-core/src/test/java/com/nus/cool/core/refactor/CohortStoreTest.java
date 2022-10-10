package com.nus.cool.core.refactor;

import com.google.common.io.Files;
import com.nus.cool.core.cohort.refactor.storage.CohortRSStr;
import com.nus.cool.core.cohort.refactor.storage.CohortWSStr;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import org.testng.annotations.Test;

public class CohortStoreTest {

  @Test()
  public void StoreCohort() throws IOException {

    System.out.println(System.getProperty("user.dir"));

    ArrayList<String> testData = new ArrayList<>(
        Arrays.asList("Userid1", "Userid2", "Userid3", "Userid4", "Userid5"));

    CohortWSStr cws = new CohortWSStr(StandardCharsets.UTF_8);

    for (String userID: testData){
      cws.addCubletResults(userID);
    }

    // output file
    String fileName = "TestCohort";
    File cubemeta = new File(System.getProperty("user.dir"), fileName);
    DataOutputStream out = new DataOutputStream(
        new FileOutputStream(cubemeta));
    cws.writeTo(out);
  }

  @Test()
  public void ReadCohort() throws IOException {
    // input file
    String fileName = "TestCohort";
    CohortRSStr crs = new CohortRSStr(StandardCharsets.UTF_8);
    File cubemeta = new File(System.getProperty("user.dir"), fileName);

//    Path filePath = Paths.get(System.getProperty("user.dir"), fileName);
//    FileChannel fc = FileChannel.open(filePath);
//    MappedByteBuffer mappedByteBuffer = fc.map(MapMode.READ_ONLY, 0, 123);
//
//    crs.readFrom(mappedByteBuffer);
//    crs.getUsers();

    crs.readFrom(Files.map(cubemeta).order(ByteOrder.nativeOrder()));
    crs.getUsers();

  }



}
