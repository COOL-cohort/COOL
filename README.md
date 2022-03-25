# COOL: a COhort OnLine analytical processing system

---

[Website](http://13.212.103.48:3001/) | [Documentation](http://13.212.103.48:3001/docs/tutorials/tutorial-csv) | [Blog](http://13.212.103.48:3001/blog) | [Demo](https://www.comp.nus.edu.sg/~dbsystem/cool/#/demo) | [GitHub](https://github.com/COOL-cohort/COOL)

---


### Introduction to COOL

![ COOL](./assets/img/p1.svg)

Different groups of people often have different behaviors or trends. For example, the bones of older people are more porous than those of younger people. It is of great value to explore the behaviors and trends of different groups of people, especially in healthcare, because we could adopt appropriate measures in time to avoid tragedy. The easiest way to do this is **cohort analysis**.

But with a variety of big data accumulated over the years, **query efficiency** becomes one of the problems that OnLine Analytical Processing (OLAP) systems meet, especially for cohort analysis. Therefore, COOL is introduced to solve the problems.

COOL is an online cohort analytical processing system that supports various types of data analytics, including cube query, iceberg query and cohort query.

With the support of several newly proposed operators on top of a sophisticated storage layer, COOL could provide high performance (near real-time) analytical response for emerging data warehouse domains.


### Key features of COOL

1. **Easy to use.** COOL is easy to deploy on local or on cloud via docker.
2. **Near Real-time Responses.** COOL is highly efficient, and therefore, can process cohort queries in near real-time analytical responses.
3. **Specialized Storage Layout.** A specialized storage layout is designed for fast query processing and reduced space consumption.
4. **Self-designed Semantics.** There are some novel self-designed semantics for the cohort query, which can simplify its complexity and improve its functionality.
5. **Flexible Integration.** Flexible integration with other data systems via common data formats(e.g., CSV, Parquet, Avro, and Arrow).
6. **Artificial Intelligence Model.** A new neural network model will be introduced soon.

### Quickstart

#### BUILD

Simply run `mvn package`

#### Required sources:

1. **dataset file**: a csv file with "," delimiter (normally dumped from a database table), and the table header is removed.
2. **dimension file**: a csv file with "," delimiter.
Each line of this file has two fields: the first field is the name of a column in the dataset, and the second field is a value of this column.
Each distinct value of each column in the dataset shall appear in this dimension file once.
3. **dataset schema file**: a `table.yaml` file specifying the dataset's columns and their measure fields.
4. **query file**: a yaml file specify the parameters for running query server.

#### Load dataset

Before query processing, we need to load the dataset into COOL native format. The sample code to load csv dataset with data loader can be found under [cool-examples/load-csv](cool-examples/load-csv/src/main/java/com/nus/cool/example/Main.java).

```
$ java -jar cool-examples/load-csv/target/load-csv-0.1-SNAPSHOT.jar path/to/your/source/directory path/to/your/.yaml path/to/your/dimensionfile path/to/your/datafile path/to/output/datasource/directory
```

The five arguments in the command have the following meaning:
1. a unique dataset name given under the directory
2. the table.yaml (the third required source)
3. the dimension file (the second required source)
4. the dataset file (the first required source)
5. the output directory for the compacted dataset

Alternatively, the data in parquet format can be loaded using the data loader with sample code under [cool-examples/load-parquet](cool-examples/load-parquet/src/main/java/com/nus/cool/example/Main.java).

```
$ java -jar cool-examples/load-parquet/target/load-parquet-0.1-SNAPSHOT.jar path/to/your/source/directory path/to/your/.yaml path/to/your/dimensionfile path/to/your/datafile path/to/output/datasource/directory
```

#### Execute queries

We provide an example for cohort query processing in [CohortLoader.java](cool-core/src/main/java/com/nus/cool/loader/CohortLoader.java).

There are two types of queries in COOL. The first one includes two steps.

- Select the specific users.

```
$ java -cp ./cool-core/target/cool-core-0.1-SNAPSHOT.jar com.nus.cool.loader.CohortCreator path/to/output/datasource/directory path/to/your/directory path/to/your/queryfile
```

- Executes cohort query on the selected users.

```
$ java -cp ./cool-core/target/cool-core-0.1-SNAPSHOT.jar com.nus.cool.loader.ExtendedCohortLoader path/to/output/datasource/directory path/to/your/directory path/to/your/queryfile
```

The second type will execute the queries on all the users.

```
$ java -cp ./cool-core/target/cool-core-0.1-SNAPSHOT.jar com.nus.cool.loader.ExtendedCohortLoader path/to/output/datasource/directory path/to/your/directory path/to/your/queryfile
```

#### Example

##### Load dataset

We have provided examples in `sogamo` directory and `health` directory. Now we take `sogamo` for example.

You can load `sogamo` dataset with the following command.

```
$ java -cp ./cool-core/target/cool-core-0.1-SNAPSHOT.jar com.nus.cool.model.CoolLoader sogamo sogamo/table.yaml sogamo/dim.csv sogamo/test.csv datasetSource
```

In addition, you can run the following command to load dataset in `parquet` format under the `sogamo` directory.

```
$ java -jar cool-extensions/parquet-extensions/target/parquet-extensions-0.1-SNAPSHOT.jar sogamo sogamo/table.yaml sogamo/dim.csv sogamo/test.parquet datasetSource
```

Finally, there will be a cube generated under the `datasetSource` directory, which is named `sogamo`.

##### Execute queries

We use the `health` dataset for example.

- Select the specific users.

```
$ java -cp ./cool-core/target/cool-core-0.1-SNAPSHOT.jar com.nus.cool.loader.CohortCreator datasetSource health health/query1-0.json
```

where the three arguments are as follows:
1. `datasetSource`: the output directory for the compacted dataset
2. `health`: the cube name of the compacted dataset
3. `health/query1-0.json`: the json file for the cohort query

- Execute cohort query on the selected users.

```
$ java -cp ./cool-core/target/cool-core-0.1-SNAPSHOT.jar com.nus.cool.loader.ExtendedCohortLoader datasetSource health health/query1-1.json
```

- Execute cohort query on all the users.

```
$ java -cp ./cool-core/target/cool-core-0.1-SNAPSHOT.jar com.nus.cool.loader.ExtendedCohortLoader datasetSource health health/query2.json
```

Partial results for the second type of query on the `health` dataset are as follows
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
* Z. Xie, H. Ying, C. Yue, M. Zhang, G. Chen, B. C. Ooi. [Cool: a COhort OnLine analytical processing system](https://www.comp.nus.edu.sg/~ooibc/icde20cool.pdf), in 2020 IEEE 36th International Conference on Data Engineering, pp.577-588, 2020.
* Q. Cai, Z. Xie, M. Zhang, G. Chen, H.V. Jagadish and B.C. Ooi. [Effective Temporal Dependence Discovery in Time Series Data](http://www.comp.nus.edu.sg/~ooibc/cohana18.pdf), in Proceedings of the VLDB Endowment, 11(8), pp.893-905, 2018.
* Z. Xie, Q. Cai, F. He, G.Y. Ooi, W. Huang, B.C. Ooi. [Cohort Analysis with Ease](https://dl.acm.org/doi/10.1145/3183713.3193540), in Proceedings of the 2018 International Conference on Management of Data, pp.1737-1740, 2018.
* D. Jiang, Q. Cai, G. Chen, H. V. Jagadish, B. C. Ooi, K.-L. Tan, and A. K. H. Tung. [Cohort Query Processing](http://www.vldb.org/pvldb/vol10/p1-ooi.pdf), in Proceedings of the VLDB Endowment, 10(1), 2016.
