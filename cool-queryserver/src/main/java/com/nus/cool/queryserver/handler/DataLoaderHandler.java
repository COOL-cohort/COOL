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

import com.nus.cool.loader.LoadQuery;
import com.nus.cool.queryserver.model.QueryServerModel;
import com.nus.cool.queryserver.utils.Util;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class DataLoaderHandler {

    @GetMapping(value = "/info")
    public String getIntroduction() {
        Util.getTimeClock();
        String text = "This is the backend for the COOL system.\n";
        text += "COOL system is a cohort OLAP system specialized for cohort analysis with extremely low latency.\n";
        text += "Workable urls: \n";
        text += "HTTP Method: GET\n";
        text += " - [server]:v1\n";
        text += " - [server]:v1/reload?cube=[cube_name]\n";
        text += " - [server]:v1/list\n";
        text += " - [server]:v1/cohort/list?cube=[cube_name]\n";
        text += "HTTP Method: POST\n";
        text += " - [server]:v1/cohort/selection\n";
        text += " - [server]:v1/cohort/analysis\n";
        text += " - [server]:v1/funnel/analysis\n";
        return text;
    }

    @PostMapping(value = "/load",
            produces = MediaType.APPLICATION_JSON_VALUE,
            consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> load(@RequestBody LoadQuery req) {
        Util.getTimeClock();
        System.out.println("[*] Server is performing the cohort query form ");
        System.out.println("[*] This query is for loading a new cube: " + req);
        return QueryServerModel.loadCube(req);
    }

//    @PostMapping(value = "/reload",
//            produces = org.springframework.http.MediaType.APPLICATION_JSON_VALUE,
//            consumes = org.springframework.http.MediaType.APPLICATION_JSON_VALUE)
//    public ResponseEntity<String> reload(@RequestBody ReloadRequest cubReq) {
//        Util.getTimeClock();
//        System.out.println("[*] Server is reloading the cube: " + cubReq.cubeName );
//        return QueryServerModel.reloadCube(cubReq.cubeName);
//    }


    @GetMapping(value = "/listcubes",
            produces = org.springframework.http.MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String[]> listCubes() {
        Util.getTimeClock();
        System.out.println("[*] Server is listing all cubes.");
        return QueryServerModel.listCubes();
    }

}



