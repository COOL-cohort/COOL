# Run the server

```bash
mvn clean install -DskipTests
java -jar cool-queryserver/target/cool-queryserver-0.1-SNAPSHOT.jar
```

# Check the [Doc](http://localhost:8080/swagger-ui/index.html#/)

# Use the Application by CURL

```bash
curl --location --request POST 'http://127.0.0.1:8080/load' \
--header 'Content-Type: application/json' \
--data '{"dataFileType": "CSV", "cubeName": "health_raw", "schemaPath": "datasets/health_raw/table.yaml", 
"dataPath": "datasets/health_raw/data.csv", "outputPath": "CubeRepo/TestCube"}'
```

```bash
curl --location --request GET 'http://localhost:8080/cohort/list_cubes'
```

```
curl --location --request GET 'http://localhost:8080/cohort/list_cube_version?cube=health_raw'
```

```
curl --location --request GET 'http://localhost:8080/cohort/list_col_info?cube=health_raw&col=birthyear'
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
--form 'queryFile=@"/Users/peng/Documents/codeRepo/KimballCai/COOL-engine/datasets/health_raw/sample_query_selection/query.json"'
```







