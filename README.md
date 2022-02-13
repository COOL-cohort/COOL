# COOL: a COhort OnLine analytical processing system

COOL is an online cohort analytical processing system that supports various types of data analytics, including cube query, iceberg query and cohort query.
The objective of COOL is to provide high performance (near real-time) analytical response for emerging data warehouse domain.

### BUILD
Simply run `mvn package`

### BEFORE QUERY
Required sources:

1. **dataset file**: a csv file with "," delimiter (normally dumped from a database table), and the table header is removed.
2. **dimension file**: a csv file with "," delimiter.
Each line of this file has two fields: the first field is the name of a column in the dataset, and the second field is a value of this column.
Each distinct value of each column in the dataset shall appear in this dimension file once.
3. **dataset schema file**: a `table.yaml` file specifying the dataset's columns and their measure fileds.
4. **query file**: a yaml file specify the parameters for running query server.
Currently, it is only required to specify the location of runtime directory (detailed in Step 2).

We have provided an example for each of the three yaml documents in sogamo directory.

Before query processing, we need to load the dataset into COOL native format. The sample code to load csv dataset with data loader can be found under [cool-examples/load-csv](cool-examples/load-csv/src/main/java/com/nus/cool/example/Main.java).
```
$ java -jar cool-examples/load-csv/target/load-csv-0.1-SNAPSHOT.jar sogamo sogamo/table.yaml sogamo/dim.csv sogamo/test.csv ./test
```
Alternatively, the same data is also in parquet format under the same folder and can be loaded using the data loader with sample code under [cool-examples/load-parquet](cool-examples/load-parquet/src/main/java/com/nus/cool/example/Main.java).
```
$ java -jar cool-examples/load-parquet/target/load-parquet-0.1-SNAPSHOT.jar sogamo sogamo/table.yaml sogamo/dim.csv sogamo/test.parquet ./test
```
The five arguments in the command have the following meaning:
1. a unique dataset name given under the output directory
2. the table.yaml (the third required source)
3. the dimension file (the second required source)
4. the dataset file (the first required source)
5. the output directory for the compacted dataset

Then, there will be a cube generated under the `test` directory, which is named `sogamo`.

Besides, we also provide another example for the `health` dataset.
```
$ java -jar cool-examples/load-csv/target/load-csv-0.1-SNAPSHOT.jar health health/table.yaml health/dim.csv health/raw.csv ./test
```

### HOW TO RUN - COHORT QUERY
We have given an example for cohort query processing in [CohortLoader.java](cool-core/src/main/java/com/nus/cool/loader/CohortLoader.java).

Execute sample query on the generated `sogamo` cube under test local repository
```
$ java -cp ./cool-core/target/cool-core-0.1-SNAPSHOT.jar com.nus.cool.loader.CohortLoader test sogamo sogamo/query0.json
```
where the three arguments are as follows:
1. `test`: the output directory for the compacted dataset
2. `sogamo`: the cube name of the compacted dataset
3. `sogamo/query0.json`: the json file for the cohort query

The sample result for the sogamo dataset is as follows
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

Besides, we also provide extended cohort queries for the `health` dataset, and we provide two types of cohort queries on the `health` cube.

In the first type, we first create specialized cohorts and then execute the designed cohort query on the selected cohort.
```
$ java -cp ./cool-core/target/cool-core-0.1-SNAPSHOT.jar com.nus.cool.loader.CohortCreator test health health/query1-0.json
$ java -cp ./cool-core/target/cool-core-0.1-SNAPSHOT.jar com.nus.cool.loader.ExtendedCohortLoader test health health/query1-1.json
```

In the second type, we directly execute the designed cohort query on all users.
```
$ java -cp ./cool-core/target/cool-core-0.1-SNAPSHOT.jar com.nus.cool.loader.ExtendedCohortLoader test health health/query2.json
```

Partial results for the second query on the `health` dataset is as follows
```
 {
  "status" : "OK",
  "elapsed" : 0,
  "result" : [ {
    "cohort" : "((1950, 1960])",
    "age" : 0,
    "measure" : 740.0,
    "min" : 45.0,
    "max" : 96.0,
    "sum" : 4516.0,
    "num" : 79.0
  }, {
    "cohort" : "((1950, 1960])",
    "age" : 1,
    "measure" : 49.0,
    "min" : 46.0,
    "max" : 72.0,
    "sum" : 981.0,
    "num" : 18.0
  }, {
    "cohort" : "((1950, 1960])",
    "age" : 2,
    "measure" : 57.0,
    "min" : 45.0,
    "max" : 81.0,
    "sum" : 2032.0,
    "num" : 37.0
  }, {
    "cohort" : "((1950, 1960])",
    "age" : 3,
    "measure" : 34.0,
    "min" : 45.0,
    "max" : 72.0,
    "sum" : 1666.0,
    "num" : 30.0
  },
  ...
```

## Publication
* Z. Xie, H. Ying, C. Yue, M. Zhang, G. Chen, B. C. Ooi. [Cool: a COhort OnLine analytical processing system](https://www.comp.nus.edu.sg/~ooibc/icde20cool.pdf) IEEE International Conference on Data Engineering, 2020
* Q. Cai, Z. Xie, M. Zhang, G. Chen, H.V. Jagadish and B.C. Ooi. [Effective Temporal Dependence Discovery in Time Series Data](http://www.comp.nus.edu.sg/~ooibc/cohana18.pdf) ACM International Conference on Very Large Data Bases (VLDB), 2018
* Z. Xie, Q. Cai, F. He, G.Y. Ooi, W. Huang, B.C. Ooi. [Cohort Analysis with Ease](https://dl.acm.org/doi/10.1145/3183713.3193540) SIGMOD Proceedings of the 2018 International Conference on Management of Data
* D. Jiang, Q. Cai, G. Chen, H. V. Jagadish, B. C. Ooi, K.-L. Tan, and A. K. H. Tung. [Cohort Query Processing](http://www.vldb.org/pvldb/vol10/p1-ooi.pdf) ACM International Conference on Very Large Data Bases (VLDB), 2016
