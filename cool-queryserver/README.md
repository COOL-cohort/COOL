


# All APIs
1. list all Cubes [GET]
 return: [list] of cube names
2. list all versions of the Cube [POST]
 param: cube name
    return: [list] of the version of cubes
2. list all cohorts [POST]
 param: cube name
    return: [list] of {dict}. dict {name: cohort name, num: cohort size, query: the query used to generate the cohort}
3. list the columns of the Cube [POST]
 param: cube name
    return: [list] of the columns
4. load a csv dataset [POST]
 param: cube name, schema file path, csv file path, cube repo name(default: COOL)
    return: error or success
5. cohort selection [POST]
 param: query (please get the cube name from the query)
    return: [list] of {dict}. dict {name: the name of the cohort; num: cohort size, query: the query used to generated the cohort}
6. cohort Anallysis [POST]
 param: query (please get the cube name from the query)
    return: {dict} of format results.

# Run the server

```bash
mvn clean package
java -jar cool-queryserver/target/cool-queryserver-0.1-SNAPSHOT.jar
```

mvn clean install -DskipTests

# Use the Application

```bash
curl --location --request POST 'http://127.0.0.1:8080/load' \
--header 'Content-Type: application/json' \
--data-raw '{"dataFileType": "CSV", "cubeName": "health_raw", "schemaPath": "datasets/health_raw/table.yaml", "dataPath": "datasets/health_raw/data.csv", "outputPath": "CubeRepo/TestCube"}
```

```bash
curl --location --request GET 'http://localhost:8080/cohort/list_cubes'
```

```
curl --location --request GET 'http://localhost:8080/cohort/list_cube_version?cube=health_raw'
```

```bash
curl --location --request GET 'http://localhost:8080/cohort/list_cube_columns?cube=health_raw'
```

```bash
curl --location --request GET 'http://localhost:8080/cohort/list_cohorts?cube=health_raw'
```

```bash
curl --location --request POST 'http://localhost:8080/cohort/cohort-analysis' \
--form 'queryFile=@"/Users/kevin/project_java/COOL/datasets/health_raw/sample_query_count/query.json"'
```

```bash
curl --location --request POST 'http://localhost:8080/cohort/selection' \
--form 'queryFile=@"/Users/kevin/project_java/COOL/datasets/health_raw/sample_query_count/query.json"'
```







