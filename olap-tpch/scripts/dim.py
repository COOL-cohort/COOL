#! /usr/bin/env python
# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# =============================================================================

import pandas as pd

data = pd.read_csv('data.csv', header=None)
data.columns = ['O_ORDERKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT', 'C_NAME', 'C_ADDRESS', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT', 'N_NAME', 'N_COMMENT', 'R_NAME', 'R_COMMENT', 'app', 'user']
columns = data.columns
for col in columns:
    a = data[col].to_list()
    a = set(a)
    with open('dim.csv', 'a+') as f:
        if col in ['O_TOTALPRICE', 'O_ORDERDATE', 'C_ACCTBAL']:
            m = max(a)
            n = min(a)
            f.write(col + ',' + str(n) + '|' + str(m) + '\n')
            continue
        for i in a:
            stri = str(i)
            if "," in stri:
                stri = stri.replace(",", ".")
            f.write(col + ',' + stri + '\n')
