# Database Setup Instructions

To create and populate a new database in PostgreSQL for testing, please follow these steps:

## Step 1: Create a Database

Create a new database in pgAdmin.

## Step 2: Create Tables

a) Import and run the `createtables.sql` file. This will create all the necessary tables.

## Step 3: Load Data

b) Run the `loaddata.py` script. This file will connect to your newly created database and load all the JSON data into those created tables from step (a).

### Note:

- You may need to modify the following parameters in `loaddata.py` to appropriate ones based on the database you have created and the parameters you used when creating it:

```python
conn = psycopg2.connect(
    host="localhost",
    database="soccerdb",
    user="postgres",
    password="1234"
)
```

- The loaddata.py script assumes that the JSON files are organized in the following directory structure:

```loaddata.py
competitions.json
matches/
    season_id/
        match_id.json
lineups/
        match_id.json
events/
        match_id.json
```
