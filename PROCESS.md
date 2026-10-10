# Jayson Yurcho Rearc Data Engineering Test
## Architecture
### Data Ingestion 
####Source to Raw
- Data is ingested from both the BLS site and the us population site via Get API calls.
- All files are saved in raw.BLS volume. 
- Metadata is stored in table in raw. 
- Upon rerun, only files with different ETags will be redownloaded. Missing Etags will go to modified date + size compairson.  
- All new files are always downloaded. 
- Pipeline located here. 

#### Raw to Bronze
Config files govern the Raw to Bronze load.

#### Bronze to Silver
Config files goveren the Bronze to Silver load. 

#### Silver to Gold (SQL)
- Config files profide SQL queries to generate tables

#### Gold
![image_1791566086863.png](./images/image_1791566086863.png "image_1791566086863.png")
## Trade-offs
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