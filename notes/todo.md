# planning
1. (done) make scripts work with new database
2. (done) add movie watch list to databse
    - (done) create auto table dump and table load script
3. (done) add figures
4. google drive
    - check if i can download and upload data via python
    - data download and updating movie list
    - uploading all files
5. automate everything daily
6. create excel file and automatically load to onedrive
7. get everything working on server
8. add jenkins cicd



# backlog

- define database (warehouse)
    - create two schema's: imdb and personal data
    - create database schema which also has the tv series in it.
    - when updateing the datasbe, just delete all data in the tables and load agian, no update scripts.
    - create a better designed datase
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
