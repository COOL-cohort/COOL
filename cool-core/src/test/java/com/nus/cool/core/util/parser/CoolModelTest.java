package com.nus.cool.core.util.parser;

import com.nus.cool.loader.CoolModel;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

public class CoolModelTest {
    //@Test
    public void testListFuncs() throws IOException {
        System.out.println(System.getProperty("user.dir"));
        File root = new File("../datasetSource");
        if(!root.exists()){
            System.out.println("Repo[../datasetSource] does not exist.");
            root.mkdir();
        }
//        String[] cubes = root.list();
//        for (String cube : cubes) {
//            System.out.println(cube);
//        }
        CoolModel model = new CoolModel("../datasetSource");
        String[] cubes2 = model.listCubes();
        System.out.println("Applications:");
        for (String cube : cubes2) {
            System.out.println(cube);
        }
    }
}
