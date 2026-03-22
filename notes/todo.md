# planning

1. create database (without relations, in imdb schema)
2. modify dataframe tables a bit more (check deletions etc from sql queries) (part of staging)
3. create table emptieing script
4. load data into database
5. create relations
6. add movie watch list to dataabse (in a new schema) (update scripts)
7. create automatic loading of onedrive data in dataabse
8. automate everything daily
9. use dbt to creatre data for excel file and figures
10. create excel file and automatically load to onedrive
11. create dashboard with usefull figures and automatically load to onedrive
12. get everything working on server
13. add jenkins cicd



# backlog

- define database (warehouse)
    - create two schema's: imdb and personal data
    - create database schema which also has the tv series in it.
    - when updateing the datasbe, just delete all data in the tables and load agian, no update scripts.
    - create a well designed datase
    - create partitions, but not indexes, because this takes forever.
    - include a script in the project that automatically can create the (empty) databse plus a user. put credentials for this in the .env file.
    - show which postgres settings to adjust for better performance.
- use polars to transform the data an load it into an (empty) database
- use dbt for querying

requirements:
- must create the movie list
- movie list must be uploaded to onedrive
- my add list must be retrieved from onedrive
- some usefull statistics must be visualized and added to onedrive
- a backup of the watch data must be added to onedrive
