### HOW TO RUN WITH A SERVER
We can start the COOL's query server with the following command
```
java -jar cool-queryserver/target/cool-queryserver-0.1-SNAPSHOT.jar datasetSource
```
where the argument is as follows:
1. `datasetSource`: the path to the directory for the compacted dataset. 

In this server, we implement many APIs and list their corresponding urls as follows:
- \[server]:v1
  - List all workable urls
- \[server]:v1/reload?cube=[cube_name]
  - Reload the cube
- \[server]:v1/list
  - List existing cubes
- \[server]:v1/cohort/list?cube=[cube_name]
  - List all cohorts from the selected cube
- \[server]:v1/cohort/create 
  - Create Cohorts
- \[server]:v1/cohort/analysis
  - Perform cohort analysis


### CONNECT TO EXTERNAL STORAGE SERVICES
COOL has an [StorageService](cool-core/src/main/java/com/nus/cool/storageservice/StorageService.java) interface, which will allow COOL standalone server/workers (coming soon) to handle data movement between local and an external storage service. A sample implementation for HDFS connection can be found under the [hdfs-extensions](cool-extensions/hdfs-extensions/).