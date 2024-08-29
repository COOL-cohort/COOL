# COOL: a COhort OnLine analytical processing system

---

[Website](https://www.comp.nus.edu.sg/~dbsystem/cool/#/) | [Documentation](https://www.comp.nus.edu.sg/~dbsystem/cool/docs/getting-started/quickstart) | [Blog](https://www.comp.nus.edu.sg/~dbsystem/cool/blog) | [Demo](http://13.212.99.209:8201/) | [GitHub](https://github.com/COOL-cohort/COOL)

---

## Introduction to COOL

![ COOL](./assets/img/arch2.png)

Different groups of people often have different behaviors or trends. For example, the bones of older people are more porous than those of younger people. It is of great value to explore the behaviors and trends of different groups of people, especially in healthcare, because we could adopt appropriate measures in time to avoid tragedy. The easiest way to do this is **cohort analysis**.

However, with a variety of big data accumulated over the years, **query efficiency** becomes one of the problems that OnLine Analytical Processing (OLAP) systems meet, especially for cohort analysis. Therefore, COOL is introduced to solve the problems.

COOL is an online cohort analytical processing system that supports various types of data analytics, including cube query, iceberg query and cohort query.

With the support of several newly proposed operators on top of a sophisticated storage layer, COOL could provide high-performance (near real-time) analytical responses for emerging data warehouse domains.

## Key features of COOL

1. **Easy to use.** COOL is easy to deploy locally or on the cloud via Docker.
2. **Near Real-time Responses.** COOL is highly efficient, and therefore, can process cohort queries in near real-time analytical responses.
3. **Specialized Storage Layout.** A specialized storage layout is designed for fast query processing and reduced space consumption.
4. **Self-designed Semantics.** There are some novel self-designed semantics for the cohort query, which can simplify its complexity and improve its functionality.
5. **Flexible Integration.** Flexible integration with other data systems via common data formats(e.g., CSV, Parquet, Avro, and Arrow).
6. **Artificial Intelligence Model.** A new neural network model will be introduced soon.

## Quickstart

### Build package

```bash
mvn clean package
```

### Required sources

1. **dataset file**: a CSV file with "," delimiter (normally dumped from a database table) and the table header removed.
2. **dataset schema file**: a `table.yaml` file specifying the dataset's columns and their measure fields.
3. **query file**: a YAML file specifying the parameters for the running query server.

### Load dataset

Before query processing, we need to load the dataset into COOL native format. The sample code to load csv dataset with data loader can be found in [CsvLoader.java](cool-core/src/main/java/com/nus/cool/functionality/CsvLoader.java).

```bash
./cool load \
    dataset \
    path/to/your/.yaml \
    path/to/your/datafile \
    path/to/output/datasource/directory
```

The five arguments in the command have the following meaning:

1. the dataset name
2. the `table.yaml` (the third required source)
3. the dataset file (the first required source)
4. the output directory for the compacted dataset

### Execute queries

We provide an example for cohort query processing in [CohortAnalysis.java](cool-core/src/main/java/com/nus/cool/functionality/CohortAnalysis.java).

<!-- There are two types of queries in COOL. The first one includes two steps. -->

#### Cohort Selection

```bash
./cool cohortselection \
    path/to/output/datasource/directory \
    path/to/your/queryfile
```

#### Cohort Query

```bash
./cool cohortquery \
    path/to/output/datasource/directory \
    path/to/your/cohortqueryfile
```

#### Funnel Query

```bash
./cool funnelquery \
    path/to/output/datasource/directory \
    path/to/your/funnelqueryfile
```

#### OLAP Query

```bash
./cool olapquery \
    path/to/output/datasource/directory \
    path/to/your/queryfile
```

## Example: Cohort Analysis

### Load dataset from different formats

We have provided examples in `sogamo` directory and `health_raw` directory. Now we take `sogamo` for example.

The COOL system supports CSV data format by default, and you can load `sogamo` dataset with the following command.

```bash
./cool load csv \
    sogamo \
    datasets/sogamo/table.yaml \
    datasets/sogamo/data.csv \
    ./CubeRepo
```

In addition, you can run the following command to load the dataset in other formats under the `sogamo` directory.

- parquet format data

```bash
./cool load parquet \
    sogamo \
    datasets/sogamo/table.yaml \
    datasets/sogamo/data.parquet \
    ./CubeRepo
```

- Arrow format data

```bash
./cool load arrow \
    sogamo \
    datasets/sogamo/table.yaml \
    datasets/sogamo/data.arrow \
    ./CubeRepo
```

- Avro format data

```bash
./cool load avro \
    sogamo \
    datasets/sogamo/table.yaml \
    datasets/sogamo/avro/test.avro \
    ./CubeRepo \
    datasets/sogamo/avro/schema.avsc
```

There will be a cube generated under the `./CubeRepo` directory, which is named `sogamo`.

Similarly, load the `health_raw` dataset with:

```bash
./cool load \
    health_raw \
    datasets/health_raw/table.yaml \
    datasets/health_raw/data.csv \
    ./CubeRepo
```

### Execute cohort queries

We use the `health_raw` dataset for example to demonstrate the cohort analysis.

#### Select the specific users

```bash
./cool cohortselection \
    ./CubeRepo \
    datasets/health_raw/sample_query_selection/query.json
```

where the arguments are:

1. `./CubeRepo`: the output directory for the compacted dataset
2. `datasets/health_raw/sample_query_selection/query.json`: the cohort query (in JSON)

<!--
- Display all selected records of the cohort in the terminal for exploration

```bash
java -cp ./cool-core/target/cool-core-0.1-SNAPSHOT.jar \
    com.nus.cool.functionality.CohortExploration \
    ./CubeRepo \
    health_raw \
    sample_query_selection
```
-->

#### Execute cohort query

```bash
./cool cohortquery \
    ./CubeRepo \
    datasets/health_raw/sample_query_average/query.json
```

#### Funnel Analysis

We use the `sogamo` dataset for example to demonstrate the funnel analysis.

```bash
./cool funnelquery \
    ./CubeRepo \
    datasets/sogamo/sample_funnel_analysis/query.json
```

## Example: OLAP Analysis

### Load OLAP dataset

We have provided examples in `olap-tpch` directory.

The COOL system supports CSV data format by default, and you can load `tpc-h` dataset with the following command.

```bash
./cool load \
    tpc-h-10g \
    datasets/olap-tpch/table.yaml \
    datasets/olap-tpch/scripts/data.csv \
    ./CubeRepo
```

Finally, there will be a cube generated under the `./CubeRepo` directory, which is named `tpc-h-10g`.

### Execute OLAP queries

Run Server

1. put the `application.property` file at the same level as the .jar file.
2. edit the server configuration in the `application.property` file.
3. run the below command.

```bash
./cool server
```

## CONNECT TO EXTERNAL STORAGE SERVICES

COOL has an [StorageService](cool-core/src/main/java/com/nus/cool/storageservice/StorageService.java) interface, which will allow COOL standalone server/workers (coming soon) to handle data movement between local and an external storage service. A sample implementation for HDFS connection can be found under the [hdfs-extensions](cool-extensions/hdfs-extensions/).

## Publication

- Q. Cai, K. Zheng, H.V. Jagadish, B.C. Ooi, J.W.L. Yip. CohortNet: Empowering Cohort Discovery for Interpretable Healthcare Analytics, in Proceedings of the VLDB Endowment, 10(17), 2024.

- Z. Xie, H. Ying, C. Yue, M. Zhang, G. Chen, B. C. Ooi. [Cool: a COhort OnLine analytical processing system](https://www.comp.nus.edu.sg/~ooibc/icde20cool.pdf), in 2020 IEEE 36th International Conference on Data Engineering, pp.577-588, 2020.
- Q. Cai, Z. Xie, M. Zhang, G. Chen, H.V. Jagadish and B.C. Ooi. [Effective Temporal Dependence Discovery in Time Series Data](http://www.comp.nus.edu.sg/~ooibc/cohana18.pdf), in Proceedings of the VLDB Endowment, 11(8), pp.893-905, 2018.
- Z. Xie, Q. Cai, F. He, G.Y. Ooi, W. Huang, B.C. Ooi. [Cohort Analysis with Ease](https://dl.acm.org/doi/10.1145/3183713.3193540), in Proceedings of the 2018 International Conference on Management of Data, pp.1737-1740, 2018.
- D. Jiang, Q. Cai, G. Chen, H. V. Jagadish, B. C. Ooi, K.-L. Tan, and A. K. H. Tung. [Cohort Query Processing](http://www.vldb.org/pvldb/vol10/p1-ooi.pdf), in Proceedings of the VLDB Endowment, 10(1), 2016.
