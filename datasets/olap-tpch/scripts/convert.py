
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
import sys

filename = sys.argv[1]
idx = filename.find(".")
namenoext = filename[0:idx]

field = ["O_ORDERKEY", "O_ORDERSTATUS", "O_TOTALPRICE", "O_ORDERDATE",
         "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT", "C_NAME",
         "C_ADDRESS", "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT",
         "N_NAME", "N_COMMENT", "R_NAME", "R_COMMENT", "app", "UserKey"]

outfile = open(namenoext + ".json", "w")
outfile.write("[\n")
lineno = 0
with open(filename) as fp:
  for line in fp:
    if lineno > 0:
      outfile.write(",\n")
    outfile.write("\t{ ")
    st = 0
    it = 0
    for i in range(0,20):
      ed = line.find(",", st)
      if i < 19 and ed == -1:
        print "tokenize error"
      if i > 0:
        outfile.write(", ")
      outfile.write("\"" + field[it] + "\":\"" + line[st:ed] + "\"")
      it = it + 1
      st = ed + 1
    outfile.write(" }")
    lineno = lineno + 1

outfile.write("\n]")
outfile.close()
print "{} lines converted.".format(lineno)
