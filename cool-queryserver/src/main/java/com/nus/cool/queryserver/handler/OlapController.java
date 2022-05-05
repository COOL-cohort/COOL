/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.nus.cool.queryserver.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.iceberg.query.IcebergQuery;
import com.nus.cool.queryserver.model.QueryServerModel;
import com.nus.cool.queryserver.utils.Util;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;


@RestController
@RequestMapping("/olap")
public class OlapController {

    @PostMapping(value = "/iceberg",
            produces = MediaType.APPLICATION_JSON_VALUE,
            consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<String> performIcebergQuery(@RequestParam("queryFile") MultipartFile queryFile) throws IOException {
        Util.getTimeClock();
        System.out.println("[*] Server is performing the cohort query form IP: ");
        System.out.println("[*] This query is for iceberg query: " + queryFile);
        String queryContent = new String(queryFile.getBytes());
        ObjectMapper mapper = new ObjectMapper();
        IcebergQuery q = mapper.readValue(queryContent, IcebergQuery.class);
        System.out.println("[*] Begin to run the query....");
        return QueryServerModel.precessIcebergQuery(q);
    }

}
