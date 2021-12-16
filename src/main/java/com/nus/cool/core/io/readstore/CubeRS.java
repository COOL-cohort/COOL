package com.nus.cool.core.io.readstore;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.nus.cool.core.schema.TableSchema;
import lombok.Getter;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.List;

public class CubeRS {

    @Getter
    private TableSchema schema;

    private List<File> cubletFiles = Lists.newArrayList();

    @Getter
    private List<CubletRS> cublets = Lists.newArrayList();

    public CubeRS(TableSchema schema) {
        this.schema = schema;
    }

    public void addCublet(File cubleFile) throws IOException {
        this.cubletFiles.add(cubleFile);
        CubletRS cubletRS = new CubletRS(this.schema);
        cubletRS.readFrom(Files.map(cubleFile).order(ByteOrder.nativeOrder()));
        cubletRS.setFile(cubleFile.getName());
        this.cublets.add(cubletRS);
    }
}
