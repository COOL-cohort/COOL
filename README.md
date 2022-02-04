# COOL: a COhort OnLine analytical processing system

COOL is an online cohort analytical processing system that supports various types of data analytics, including cube query, iceberg query and cohort query.
The objective of COOL is to provide high performance (near real-time) analytical response for emerging data warehouse domain.

### BUILD
Simply run `mvn package`

### BEFORE QUERY
Required sources:

1. **dataset**: a csv file with "," delimiter (normally dumped from a database table)
2. **dimension file**: a csv file with "," delimiter.
Each line of this file has two fields: the first field is the name of a column in the dataset, and the second field is a value of this column.
Each distinct value of each column in the dataset shall appear in this dimension file once.
3. **schema file**: a json file describing the schema of the dataset.
4. **cube schema**: a json file specifying the dimension and measure fileds (Optional).
5. **query file**: a yaml file specify the parameters for running query server.
Currently, it is only required to specify the location of runtime directory (detailed in Step 2).

We have provided an example for each of the three yaml documents in sogamo directory.

Before query processing, we need to load the dataset into COOL native format. The sample code to load csv dataset with data loader can be found under [cool-examples/load-csv](cool-examples/load-csv/src/main/java/com/nus/cool/example/Main.java).
```
$ java -jar cool-examples/load-csv/target/load-csv-0.1-SNAPSHOT.jar sogamo sogamo/table.yaml sogamo/dim_test.csv sogamo/test.csv ./test
```
Alternatively, the same data is also in parquet format under the same folder and can be loaded using the data loader with sample code under [cool-examples/load-parquet](cool-examples/load-parquet/src/main/java/com/nus/cool/example/Main.java).
```
$ java -jar cool-examples/load-parquet/target/load-parquet-0.1-SNAPSHOT.jar sogamo sogamo/table.yaml sogamo/dim_test.csv sogamo/test.parquet ./test
```
The five arguments in the command have the following meaning:
1. a unique dataset name given under the output directory
2. the table.yaml (the third required source)
3. the dimension file (the second required source)
4. the dataset file (the first required source)
5. the output directory for the compacted dataset

### HOW TO RUN - COHORT QUERY
We have given an example for cohort query processing in [CohortLoader.java](src/main/java/com/nus/cool/loader/CohortLoader.java).

Load sample sogamo csv data with DataLoader as described above.

Execute sample query on the generated sogamo cube under test local repository
```
$ java -cp ./cool-core/target/cool-core-0.1-SNAPSHOT.jar com.nus.cool.loader.CohortLoader test sogamo
```
The sample result is as follows
```
{
  "status" : "OK",
  "elapsed" : 0,
  "result" : [ {
    "cohort" : "null",
    "age" : 0,
    "measure" : 2
  }, {
    "cohort" : "null",
    "age" : 1,
    "measure" : 2
  }, {
    "cohort" : "United States",
    "age" : 0,
    "measure" : 2
  }, {
    "cohort" : "United States",
    "age" : 1,
    "measure" : 2
  }, {
    "cohort" : "Australia",
    "age" : 0,
    "measure" : 1
  }, {
    "cohort" : "Australia",
    "age" : 1,
    "measure" : 1
  } ]
}
```

## Publication
* Z. Xie, H. Ying, C. Yue, M. Zhang, G. Chen, B. C. Ooi. [Cool: a COhort OnLine analytical processing system](https://www.comp.nus.edu.sg/~ooibc/icde20cool.pdf) IEEE International Conference on Data Engineering, 2020
* Q. Cai, Z. Xie, M. Zhang, G. Chen, H.V. Jagadish and B.C. Ooi. [Effective Temporal Dependence Discovery in Time Series Data](http://www.comp.nus.edu.sg/~ooibc/cohana18.pdf) ACM International Conference on Very Large Data Bases (VLDB), 2018
* Z. Xie, Q. Cai, F. He, G.Y. Ooi, W. Huang, B.C. Ooi. [Cohort Analysis with Ease](https://dl.acm.org/doi/10.1145/3183713.3193540) SIGMOD Proceedings of the 2018 International Conference on Management of Data
* D. Jiang, Q. Cai, G. Chen, H. V. Jagadish, B. C. Ooi, K.-L. Tan, and A. K. H. Tung. [Cohort Query Processing](http://www.vldb.org/pvldb/vol10/p1-ooi.pdf) ACM International Conference on Very Large Data Bases (VLDB), 2016
