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

package com.nus.cool.functionality;
import com.nus.cool.core.iceberg.query.IcebergQuery;
import com.nus.cool.core.iceberg.result.BaseResult;
import com.nus.cool.model.CoolModel;
import java.util.List;


public class RelationalAlgebra {

   public static void main(String[] args) throws Exception {
       // the path of dz file eg "COOL/cube"
       String dzFilePath = args[0];
       String dataSourceName = args[1];
       String operation = args[2];

       // load .dz file
       CoolModel coolModel = new CoolModel(dzFilePath);
       coolModel.reload(dataSourceName);

       IcebergQuery query = coolModel.olapEngine.generateQuery(operation, dataSourceName);
       if (query == null){
           return;
       }

       // execute query
       List<BaseResult> result = coolModel.olapEngine.performOlapQuery(coolModel.getCube(dataSourceName), query);
       System.out.println(result.toString());
   }
}
