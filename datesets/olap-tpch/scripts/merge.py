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

customer = pd.read_csv('customer.tbl', sep='|', header=None)
customer.columns = ['CUSTKEY', 'C_NAME', 'C_ADDRESS', 'NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT', 'Unnamed1']
nation = pd.read_csv('nation.tbl', sep='|', header=None)
nation.columns = ['NATIONKEY', 'N_NAME', 'REGIONKEY', 'N_COMMENT', 'Unnamed2']
orders = pd.read_csv('orders.tbl', sep='|', header=None)
orders.columns = ['O_ORDERKEY', 'CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT', 'Unnamed3']
region = pd.read_csv('region.tbl', sep='|', header=None)
region.columns = ['REGIONKEY', 'R_NAME', 'R_COMMENT', 'Unnamed4']

data = orders.merge(customer, on='CUSTKEY')
data = data.merge(nation, on='NATIONKEY')
data = data.merge(region, on='REGIONKEY')

print(data.columns)

for col in data.columns:
    if 'Unnamed' in col:
        print(col)
        data = data.drop([col], axis=1)
        print(data.columns)

data =  data.drop(['CUSTKEY', 'NATIONKEY', 'REGIONKEY'], axis=1)

data['O_TOTALPRICE'] = data['O_TOTALPRICE'].astype('int')
data['C_ACCTBAL'] = data['C_ACCTBAL'].astype('int')
data['app'] = 'd8273h4'
data['user'] = 'dh283gh2'
data = data.sort_values(by=['O_ORDERDATE'])

data["O_COMMENT"] = data["O_COMMENT"].str.replace(",", ".")
data["C_ADDRESS"] = data["C_ADDRESS"].str.replace(",", ".")
data["C_COMMENT"] = data["C_COMMENT"].str.replace(",", ".")
data["N_COMMENT"] = data["N_COMMENT"].str.replace(",", ".")
data["R_COMMENT"] = data["R_COMMENT"].str.replace(",", ".")

data.to_csv('data.csv', index=False, header=None)

