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

Before query processing, we need to load the dataset file (i.e.,csv data) with LocalLoader that is to compact the dataset with the following command:
```
// Generate sogamo dz dataset
java -cp ./target/cool-0.1-SNAPSHOT.jar com.nus.cool.loader.LocalLoader sogamo sogamo/table.yaml sogamo/dim_test.csv sogamo/test.csv ./test 65536
// Generate health dz dataset
java -cp ./target/cool-0.1-SNAPSHOT.jar com.nus.cool.loader.LocalLoader health health/table.yaml health/dim.csv health/raw.csv ./test 65536
```
where the five arguments are as follows:
1. `sogamo`: the cube name
2. `sogamo/table.yaml`: the dataset schema file (the third required source)
3. `sogamo/dim_test.csv`: the dimension file (the second required source)
4. `sogamo/test.csv`: the dataset file (the first required source)
5. `./test`: the output directory for the compacted dataset
6. `65536`: the chunk size

Then, there will be a cube generated under the `test` directory, which is named `sogamo`.

### HOW TO RUN - COHORT QUERY
We have given an example for cohort query processing in [CohortLoader.java](src/main/java/com/nus/cool/loader/CohortLoader.java).

Execute sample query on the generated sogamo cube under test local repository
```
// run sogamo query 
java -cp ./target/cool-0.1-SNAPSHOT.jar com.nus.cool.loader.CohortLoader test sogamo sogamo/query0.json
// run health query 
java -cp ./target/cool-0.1-SNAPSHOT.jar com.nus.cool.loader.CreateCohort test health health/query0.json
java -cp ./target/cool-0.1-SNAPSHOT.jar com.nus.cool.loader.ExtendedCohortLoader test health health/query1.json

```
where the two arguments are as follows:
1. `test`: the output directory for the compacted dataset
2. `sogamo`: the cube name

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
