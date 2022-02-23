package com.nus.cool.core.util.parser;

import com.nus.cool.loader.CoolModel;
import org.testng.annotations.Test;

import java.io.File;

public class CoolModelTest {
    @Test
    public void testListFuncs(){
        CoolModel model = new CoolModel("../datasetSource");
        String[] cubes = model.listCubes();
//        System.out.println(System.getProperty("user.dir"));
//        String[] cubes = new File("../datasetSource").list();
        for (String cube : cubes) {
            System.out.println(cube);
        }
    }
}
