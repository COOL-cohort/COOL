# COOL: a COhort OnLine analytical processing system

COOL is an online cohort analytical processing system that supports various types of data analytics, including cube query, iceberg query and cohort query.
The objective of COOL is to provide high performance (near real-time) analytical response for emerging data warehouse domain.

### BUILD
Simply run `mvn package`

### BEFORE QUERY
Required sources:

1. **dataset**: a csv file with "," delimiter (normally dumped from a database
        table)
2. **dimension file**: a csv file with "," delimiter.
Each line of this file has two fields: the first field is the name of a column in the dataset, and the second field is a value of this column.
Each distinct value of each column in the dataset shall appear in this dimension file once.
3. **schema file**: a json file describing the schema of the dataset.
4. **cube schema**: a json file specifying the dimension and measure fileds (Optional).
5. **query file**: a yaml file specify the parameters for running query server.
Currently, it is only required to specify the location of runtime directory (detailed in Step 2).

We have provided an example for each of the three yaml documents in sogamo directory.

Before query processing, we need to compact the dataset with the following command:
`java -jar cohana-loader.jar /path/to/table.yaml /path/to/dimension.csv /path/to/dataset.csv /path/to/output/directory 65536`
where the five arguments are as follows:
1. the table.yaml (the third required source)
2. the dimension file (the second required source)
3. the dataset file (the first required source)
4. the output directory for the compacted dataset
5. the chunk size

### HOW TO RUN - COHORT QUERY
We have given an example for cohort query processing in [CohortLoader.java](src/main/java/com/nus/cool/loader/CohortLoader.java).


