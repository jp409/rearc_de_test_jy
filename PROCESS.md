# Jayson Yurcho Rearc Data Engineering Test
## Architecture
The architecture of this data pipeline follows a tradtional medallion architecture.
![image_1791642328437.png](./image_1791642328437.png "image_1791642328437.png")
Source - In this case, the BLS data folder and US Popilation AIP
Raw - Databricks volume located at workspace.raw.bls which stores raw files ingestes from sources. 
Bronze - A 1:1 copy of the source data. Schema and data are unchanged. 
Silver - Cleaned version of data. Null checks are performed here. 
Gold - Transformed and enriched version of data. 
![image_1791642836338.png](./image_1791642836338.png "image_1791642836338.png")
### Data Ingestion 
####Source to Raw
- Data is ingested from both the BLS site and the us population site via Get API calls.
- All files are saved in raw.BLS volume. 
- Metadata is stored in table in raw. 
![image_1791642737587.png](./image_1791642737587.png "image_1791642737587.png")
- Upon rerun, only files with different ETags will be redownloaded. Missing Etags will go to modified date + size compairson.  
- All new files are always downloaded. 
- Pipeline located here. 

#### Raw to Bronze
Config files govern the Raw to Bronze load.

#### Bronze to Silver
Config files goveren the Bronze to Silver load. 

#### Silver to Gold (SQL)
![image_1791642902572.png](./image_1791642902572.png "image_1791642902572.png")
- Config files profide SQL queries to generate tables

#### Gold
![image_1791566086863.png](./images/image_1791566086863.png "image_1791566086863.png")
## Trade-offs
- Data could be appended in bronze, thereby creating historic snapshots. 
## AI Usage
AI was used to 
## Retrospective
## Answers
###PySpark
1. answer
2. answer
3. answer

###SQL
1. answer
2. answer
3. answer