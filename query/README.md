# Test Cohort Query

Every sub-directory contains a cohort query for unit test.

The rule of naming sub-directory is join the \"query\" , the name of source dataset and the number of query.


The sub-directory layout, for example.

```
--- query_health_1 
     --- generateResult.py
     --- query.json
     --- result.json
     --- README.md
```


`GenerateResult.py`  is the script to generate the true value saved in `result.json` file

`README.md` explain the intention of this cohort analysis.

`query.json` is the cohort query.
