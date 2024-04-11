If you are creating create a new db in postgres from scrach to test, please follow these steps:

Create a db in pg admin.

a) Import and run the createtables.sql file. This will create all the necessary tables.

b) Run the loaddata.by. This file will connect to your newly created db 
and will load all the json data into those created tables via (a). 

Note: 
1. You may need to modify the following paramenters in loaddata.by to approriate ones based 
on the your created database and parameters you have used when creating it:
conn = psycopg2.connect(
    host="localhost",
    database="soccerdb",
    user="postgres",
    password="1234"
)

2. The loaddata.py script assumes that the JSON files are organized in the following directory structure:

loaddata.py

competitions.json
matches/
  season_id/
      match_id.json
lineups/
      match_id.json
events/
      match_id.json 