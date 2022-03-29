# DataSet preparation

## TPC-H dataset

1. Download from [data source](https://www.tpc.org/tpc_documents_current_versions/download_programs/tools-download-request5.asp?bm_type=TPC-H&bm_vers=3.0.0&mode=CURRENT-ONLY) and extract from data from the tic zip file.

2. In `/TPC-H_Tools_v3.0.0/dbgen`  run

   ```bash
   cp makefile.suite makefile
   ```

3. Open the Makefile, find and modify the following lines (103~111)

   ```c++
   ################
   ## CHANGE NAME OF ANSI COMPILER HERE
   ################
   CC      = gcc 
   # Current values for DATABASE are: INFORMIX, DB2, TDAT (Teradata)
   #                                  SQLSERVER, SYBASE, ORACLE, VECTORWISE
   # Current values for MACHINE are:  ATT, DOS, HP, IBM, ICL, MVS, 
   #                                  SGI, SUN, U2200, VMS, LINUX, WIN32 
   # Current values for WORKLOAD are:  TPCH
   DATABASE= SQLSERVER
   MACHINE = LINUX
   WORKLOAD = TPCH
   ```

4. For Mac users,  update package name from 

   ```c
   #include <malloc.h>
   ```

   to

   ```c
   #include <sys/malloc.h>
   ```

   for each file. 

5. Compile with

   ```bash
   make
   ```

6. In `/TPC-H_Tools_v3.0.0/dbgen` , generate 4 table namely `nation.tbl, region.tbl, orders.tbl, customer.tbl` with 

   ```bash
   ./dbgen -f -s 10
   ```
