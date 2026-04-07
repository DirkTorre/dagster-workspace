# planning
1. make scripts work with new database
    - (done) create database connection class with methods
    - (done) add the new database connection to dagster
    - (done) implement the new class to load data
    - create assets for relationship building
    - modify postgres resource to work with dagster resources
    - create log for loading data
2. modify database to have (sorted) partitions (for large (and medium) tables) (remember that i have less ram to work with on my server)
5. create assets that create and remove relationships. every relationship will become it's own asset (but can be grouped a bit if possible)
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
