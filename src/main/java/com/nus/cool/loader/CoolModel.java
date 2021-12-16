package com.nus.cool.loader;

import com.google.common.collect.Maps;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.schema.TableSchema;

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class CoolModel implements Closeable {

    private Map<String, CubeRS> metaStore = Maps.newHashMap();

    private File localRepo;

    public CoolModel(String path) {
        this.localRepo = new File(path);
    }

    public synchronized void reload(String cube) throws IOException {
        this.metaStore.remove(cube);
        File cubeRoot = new File(this.localRepo, cube);
        if (!cubeRoot.exists())
            throw new FileNotFoundException(cube + " was not found");
        TableSchema schema = TableSchema.read(new FileInputStream(new File(cubeRoot, "table.yaml")));
        CubeRS cubeRS = new CubeRS(schema);
        File[] versions = cubeRoot.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.isDirectory();
            }
        });
        checkNotNull(versions);
        if (versions.length == 0)
            return;
        Arrays.sort(versions);
        File currentVersion = versions[versions.length - 1];
        File[] cubletFiles = currentVersion.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File file, String s) {
                return s.endsWith(".dz");
            }
        });
        checkNotNull(cubletFiles);
        for (File cubletFile : cubletFiles)
            cubeRS.addCublet(cubletFile);
        this.metaStore.put(cube, cubeRS);
    }

    @Override
    public void close() throws IOException {

    }

    public synchronized CubeRS getCube(String cube) {
        return this.metaStore.get(cube);
    }
}
