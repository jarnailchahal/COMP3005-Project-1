import json
import os
import psycopg2

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="soccerdb",
    user="postgres",
    password="1234"
)

# Create a cursor object to execute SQL queries
cur = conn.cursor()

# Load and insert data from competitions.json
with open("competitions.json") as file:
    competitions_data = json.load(file)

    print("updating competitions data...")


    for competition in competitions_data:
        if (competition["competition_name"] == "Premier League" and competition["season_name"] == "2003/2004") or \
        (competition["competition_name"] == "La Liga" and competition["season_name"] in ["2020/2021", "2019/2020", "2018/2019"]):
            
            # Insert season if it doesn't exist
            cur.execute("""
                INSERT INTO seasons (season_id, season_name)
                VALUES (%s, %s)
                ON CONFLICT (season_id) DO NOTHING
            """, (competition["season_id"], competition["season_name"]))
            
            
            # Insert competition with the season ID
            cur.execute("""
                INSERT INTO competitions (
                    competition_id, country_name, competition_name,
                    competition_gender, competition_youth, competition_international, season_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                competition["competition_id"], competition["country_name"],
                competition["competition_name"], competition["competition_gender"],
                competition["competition_youth"], competition["competition_international"],
                competition["season_id"]
            ))
        

# Load and insert data from matches JSON files
matches_directory = "matches"

# This directory contains subdirectories for each `competition_id`, and within those, JSON files for 
# each `season_id` containing match data.
   
for season_directory in os.listdir(matches_directory):
    season_path = os.path.join(matches_directory, season_directory)
    
    if season_directory.startswith("."):
        continue  # Skip hidden files like .DS_Store

    print("checking matches data..." + season_path + "  Competition ID:" + season_directory)

    for match_file in os.listdir(season_path):
        match_path = os.path.join(season_path, match_file)

        with open(match_path) as file:
            match_data = json.load(file)

            for match in match_data:
                
                if (match["competition"]["competition_name"] == "Premier League" and match["season"]["season_name"] == "2003/2004") or \
                   (match["competition"]["competition_name"] == "La Liga" and match["season"]["season_name"] in ["2020/2021", "2019/2020", "2018/2019"]):
                        
                    print("  Competition + Season good...inserting data from file..." + match_path + " (Competition_name:'" + match["competition"]["competition_name"] + "' & Season_name :'" + match["season"]["season_name"] + "')" )

                    cur.execute("""
                        SELECT competition_id FROM competitions WHERE competition_id = %s
                    """, (match["competition"]["competition_id"],))

                    competition_id = cur.fetchone()[0]

                    cur.execute("""
                        SELECT season_id FROM seasons WHERE season_id = %s
                    """, (match["season"]["season_id"],))

                    season_id = cur.fetchone()[0]

                    # Insert home team country in the countries table
                    cur.execute("""
                        INSERT INTO countries (country_id, country_name)
                        VALUES (%s, %s)
                        ON CONFLICT (country_id) DO NOTHING
                    """, (
                        match["home_team"]["country"]["id"], match["home_team"]["country"]["name"]
                    ))

                    # Insert away team country in the countries table
                    cur.execute("""
                        INSERT INTO countries (country_id, country_name)
                        VALUES (%s, %s)
                        ON CONFLICT (country_id) DO NOTHING
                    """, (
                        match["away_team"]["country"]["id"], match["away_team"]["country"]["name"]
                    ))

                    cur.execute("""
                        INSERT INTO teams (team_id, team_name, team_gender, team_group, country_id)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (team_id) DO NOTHING
                    """, (
                        match["home_team"]["home_team_id"], match["home_team"]["home_team_name"],
                        match["home_team"]["home_team_gender"], match["home_team"]["home_team_group"],
                        match["home_team"]["country"]["id"]
                    ))

                    cur.execute("""
                        INSERT INTO teams (team_id, team_name, team_gender, team_group, country_id)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (team_id) DO NOTHING
                    """, (
                        match["away_team"]["away_team_id"], match["away_team"]["away_team_name"],
                        match["away_team"]["away_team_gender"], match["away_team"]["away_team_group"],
                        match["away_team"]["country"]["id"]
                    ))

                    referee_id = None
                    stadium_id = None

                    if "referee" in match:
                        # Insert referee country in the countries table
                        cur.execute("""
                            INSERT INTO countries (country_id, country_name)
                            VALUES (%s, %s)
                            ON CONFLICT (country_id) DO NOTHING
                        """, (
                            match["referee"]["country"]["id"], match["referee"]["country"]["name"]
                        ))

                        cur.execute("""
                            INSERT INTO referees (referee_id, referee_name, country_id)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (referee_id) DO NOTHING
                            RETURNING referee_id
                        """, (
                            match["referee"]["id"], match["referee"]["name"],
                            match["referee"]["country"]["id"]
                        ))
                        referee_id = cur.fetchone()[0] if cur.rowcount > 0 else None
                    

                    if "stadium" in match:
                        
                        # Insert stadium country in the countries table
                        cur.execute("""
                            INSERT INTO countries (country_id, country_name)
                            VALUES (%s, %s)
                            ON CONFLICT (country_id) DO NOTHING
                        """, (
                            match["stadium"]["country"]["id"], match["stadium"]["country"]["name"]
                        ))

                        # Insert stadium in the stadiums table
                        cur.execute("""
                            INSERT INTO stadiums (stadium_id, stadium_name, country_id)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (stadium_id) DO NOTHING
                            RETURNING stadium_id
                        """, (
                            match["stadium"]["id"], match["stadium"]["name"],
                            match["stadium"]["country"]["id"]
                        ))
                        stadium_id = cur.fetchone()[0] if cur.rowcount > 0 else None
                    
                    
                    cur.execute("""
                        INSERT INTO matches (
                            match_id, match_date, kick_off, competition_id, season_id,
                            home_team_id, away_team_id, home_score, away_score,
                            match_week, competition_stage_id, stadium_id, referee_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        match["match_id"], match["match_date"], match["kick_off"],
                        competition_id, season_id,
                        match["home_team"]["home_team_id"], match["away_team"]["away_team_id"],
                        match["home_score"], match["away_score"], match["match_week"],
                        match["competition_stage"]["id"], stadium_id, referee_id
                    ))




# Load and insert data from lineups JSON files
lineups_directory = "lineups"

for lineup_file in os.listdir(lineups_directory):
    lineup_path = os.path.join(lineups_directory, lineup_file)
    match_id = int(lineup_file.split(".")[0])  # Since the file name is the match ID
    
    # Since we dont need to add all matches and only need matches associated with competitions and seasons 
    # we are interested in which are 'La Liga' and 'Premier League'

    # if the match id is not in the matches table, skip inserting the lineup data
    cur.execute(""" SELECT match_id FROM matches WHERE match_id = %s """, (match_id,))
    if cur.fetchone() is None:
        continue

    with open(lineup_path) as file:
        lineup_data = json.load(file)

        print("inserting lineup data... " + lineup_path + "  (Match ID:" + str(match_id) + ")" )

        for team_lineup in lineup_data:

            print("  Team ID:" + str(team_lineup["team_id"]) + "  Team Name:" + team_lineup["team_name"])
            
            team_id = team_lineup["team_id"]
            team_name = team_lineup["team_name"]
            # team_gender = None
            # team_group = None
            # country_id = None
            # country_name = None
            
            #check if team_id exists in the teams table
            cur.execute(""" SELECT team_id FROM teams WHERE team_id = %s """, (team_id,))
            if cur.fetchone() is None:
                print("Team ID not found in teams table..." + str(team_id))
                # this should not happen, but if it does, exit the script
                exit()
            
            for player in team_lineup["lineup"]:
                
                # Insert or update player information
                country_id = player["country"]["id"] if "country" in player and "id" in player["country"] else None
                country_name = player["country"]["name"] if "country" in player and "name" in player["country"] else None

                # Insert player country in the countries table if it doesn't exist
                if country_id is not None:
                    cur.execute("""
                        INSERT INTO countries (country_id, country_name)
                        VALUES (%s, %s)
                        ON CONFLICT (country_id) DO NOTHING
                    """, (country_id, country_name))


                cur.execute("""
                    INSERT INTO players (player_id, player_name, player_nickname, country_id)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (player_id) DO NOTHING
                """, (
                    player["player_id"], player["player_name"], player["player_nickname"],
                    country_id
                ))
                
                # Link player to the match and team
                cur.execute("""
                    INSERT INTO player_teams (player_id, team_id, jersey_number)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (
                    player["player_id"], team_id, player["jersey_number"]
                ))


                # 1. Insert players position information
                position_ids = set()
                
                # Insert position information
                for position in player["positions"]:
                    position_ids.add((position["position_id"], position["position"]))
                    
                # Insert position_id
                for position_id in position_ids:
                    cur.execute("""
                        INSERT INTO position_types (position_id, position_name)
                        VALUES (%s, %s)
                        ON CONFLICT (position_id) DO NOTHING;
                    """, position_id)

                # Insert player position information
                for position in player["positions"]:
                    cur.execute("""
                        INSERT INTO positions (player_id, match_id, position_id, "from", "to", from_period, to_period, start_reason, end_reason)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, (
                        player["player_id"], match_id, position["position_id"], position["from"], 
                        position["to"], position["from_period"], position["to_period"], position["start_reason"], position["end_reason"]
                    ))
            
                # 2. Insert cards information
                card_types = set()
                reason_types = set()

                # Insert card types and reason types
                for card in player["cards"]:
                    card_types.add(card["card_type"])
                    reason_types.add(card["reason"])

                # Insert card types
                for card_type in card_types:
                    cur.execute("""
                        INSERT INTO card_types (card_type_name)
                        VALUES (%s)
                        ON CONFLICT (card_type_name) DO NOTHING;
                    """, (card_type,))

                # Insert reason types
                for reason_type in reason_types:
                    cur.execute("""
                        INSERT INTO reason_types (reason_type_name)
                        VALUES (%s)
                        ON CONFLICT (reason_type_name) DO NOTHING;
                    """, (reason_type,))

                # Insert card information
                for card in player["cards"]:
                    cur.execute("""
                        INSERT INTO cards (player_id, match_id, card_type_id, reason_type_id, time, period)
                        VALUES (
                            %s, %s,
                            (SELECT card_type_id FROM card_types WHERE card_type_name = %s),
                            (SELECT reason_type_id FROM reason_types WHERE reason_type_name = %s),
                            %s, %s
                        );
                    """, (
                        player["player_id"], match_id, card["card_type"], card["reason"],
                        card["time"], card["period"]
                    ))
                
                    


# Load and insert data from events JSON files
events_directory = "events"

for event_file in os.listdir(events_directory):
    event_path = os.path.join(events_directory, event_file)
    match_id = int(event_file.split(".")[0])  # Since the file name is the match ID

    # if match id is not in the matches table, skip inserting the event data
    cur.execute(""" SELECT match_id FROM matches WHERE match_id = %s """, (match_id,))
    if cur.fetchone() is None:
        continue

    with open(event_path) as file:
        event_data = json.load(file)

        print("updating events data... " + event_path + "  (Match ID:" + str(match_id) + ")" )

        # Insert event types into event_types table
        event_types = set()
        for event in event_data:
            event_types.add((event["type"]["id"], event["type"]["name"]))

        for event_type in event_types:
            cur.execute("""
                INSERT INTO event_types (type_id, type_name)
                VALUES (%s, %s)
                ON CONFLICT (type_id) DO NOTHING;
            """, event_type)

        # Insert play patterns into play_patterns table
        play_patterns = set()
        for event in event_data:
            play_patterns.add((event["play_pattern"]["id"], event["play_pattern"]["name"]))

        for play_pattern in play_patterns:
            cur.execute("""
                INSERT INTO play_patterns (pattern_id, pattern_name)
                VALUES (%s, %s)
                ON CONFLICT (pattern_id) DO NOTHING;
            """, play_pattern)

        # Insert positions into position types table
        positions = set()
        for event in event_data:
            if "position" in event:
                positions.add((event["position"]["id"], event["position"]["name"]))
        
        for position in positions:
            cur.execute("""
                INSERT INTO position_types (position_id, position_name)
                VALUES (%s, %s)
                ON CONFLICT (position_id) DO NOTHING;
            """, position)
        

        # Insert pass heights
        pass_heights = set()
        for event in event_data:
            if "pass" in event:
                pass_heights.add((event["pass"]["height"]["id"], event["pass"]["height"]["name"]))

        for pass_height in pass_heights:
            cur.execute("""
                INSERT INTO pass_heights (pass_height_id, pass_height_name)
                VALUES (%s, %s)
                ON CONFLICT (pass_height_id) DO NOTHING;
            """, pass_height)

        # Insert body parts
        body_parts = set()
        for event in event_data:
            if "pass" in event:
                if "body_part" in event["pass"]:
                    body_parts.add((event["pass"]["body_part"]["id"], event["pass"]["body_part"]["name"]))

                for body_part in body_parts:
                    cur.execute("""
                        INSERT INTO body_parts (body_part_id, body_part_name)
                        VALUES (%s, %s)
                        ON CONFLICT (body_part_id) DO NOTHING;
                    """, body_part)



        for event in event_data:

            # Check if match_id exists in the matches table
            cur.execute("""
                SELECT match_id FROM matches WHERE match_id = %s
            """, (match_id,))

            if cur.fetchone() is None:
                # this should not happen, but if it does, exit the script
                print("Exiting..Match ID not found in matches table..." + str(match_id))
                exit()

            tactics_formation = event.get("tactics", {}).get("formation")  # Returns None if "tactics" or "formation" key is not present
            duration = event.get("duration")  # Get the duration value, returns None if it doesn't exist

            statsbomb_xg = event.get("shot", {}).get("statsbomb_xg")  # Extract xG if present. Returns None if "shot" or "statsbomb_xg" key is not present



            cur.execute("""
                INSERT INTO events (
                    event_id, match_id, event_index, period, "timestamp", minute, second,
                    type_id, possession, possession_team_id, play_pattern_id, team_id,
                    player_id, position_id, location_x, location_y, duration, tactics_formation
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                event["id"], match_id, event["index"], event["period"], event["timestamp"],
                event["minute"], event["second"], event["type"]["id"], event["possession"],
                event["possession_team"]["id"], event["play_pattern"]["id"], event["team"]["id"],
                event["player"]["id"] if "player" in event else None,
                event["position"]["id"] if "position" in event else None,
                event["location"][0] if "location" in event else None,
                event["location"][1] if "location" in event else None,
                event["duration"] if "duration" in event else None,
                event["tactics"]["formation"] if "tactics" in event else None
            ))

            if "related_events" in event:
                for related_event_id in event["related_events"]:
                    cur.execute("""
                        INSERT INTO related_events (event_id, related_event_id)
                        VALUES (%s, %s);
                    """, (event["id"], related_event_id))

            if "pass" in event:

                recipient_player_id = event["pass"]["recipient"]["id"] if "recipient" in event["pass"] else None
                pass_length = event["pass"]["length"] if "length" in event["pass"] else None
                pass_angle = event["pass"]["angle"] if "angle" in event["pass"] else None
                pass_height_id = event["pass"]["height"]["id"] if "height" in event["pass"] else None
                end_location_x = event["pass"]["end_location"][0] if "end_location" in event["pass"] else None
                end_location_y = event["pass"]["end_location"][1] if "end_location" in event["pass"] else None
                body_part_id = event["pass"]["body_part"]["id"] if "body_part" in event["pass"] else None
                through_ball = event["pass"]["through_ball"] if "through_ball" in event["pass"] else None
                shot_assist = event["pass"]["shot_assist"] if "shot_assist" in event["pass"] else None

                cur.execute("""
                    INSERT INTO passes (
                        event_id, recipient_player_id, pass_length, pass_angle,
                        pass_height_id, end_location_x, end_location_y, body_part_id, through_ball, shot_assist
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (
                    event["id"],
                    recipient_player_id,
                    pass_length,
                    pass_angle,
                    pass_height_id,
                    end_location_x,
                    end_location_y,
                    body_part_id, 
                    through_ball,
                    shot_assist
                ))

            # Insert shot outcomes
        shot_outcomes = set()
        for event in event_data:
            if "shot" in event:
                shot_outcomes.add((event["shot"]["outcome"]["id"], event["shot"]["outcome"]["name"]))

        for outcome in shot_outcomes:
            cur.execute("""
                INSERT INTO shot_outcomes (outcome_id, outcome_name)
                VALUES (%s, %s)
                ON CONFLICT (outcome_id) DO NOTHING;
            """, outcome)

        # Insert shot types
        shot_types = set()
        for event in event_data:
            if "shot" in event:
                shot_types.add((event["shot"]["type"]["id"], event["shot"]["type"]["name"]))

        for shot_type in shot_types:
            cur.execute("""
                INSERT INTO shot_types (type_id, type_name)
                VALUES (%s, %s)
                ON CONFLICT (type_id) DO NOTHING;
            """, shot_type)

        # Insert shot techniques
        shot_techniques = set()
        for event in event_data:
            if "shot" in event:
                shot_techniques.add((event["shot"]["technique"]["id"], event["shot"]["technique"]["name"]))

        for technique in shot_techniques:
            cur.execute("""
                INSERT INTO shot_techniques (technique_id, technique_name)
                VALUES (%s, %s)
                ON CONFLICT (technique_id) DO NOTHING;
            """, technique)

        # Insert shots
        for event in event_data:
            
            if "shot" in event:
                
                cur.execute("""
                    INSERT INTO shots (
                        event_id, statsbomb_xg, end_location_x, end_location_y,
                        key_pass_id, outcome_id, first_time, type_id, technique_id, body_part_id
                    )
                    VALUES (%s, %s, %s,  %s, %s, %s, %s, %s, %s, %s)
                    RETURNING shot_id;
                """, (
                    event["id"], event["shot"]["statsbomb_xg"],
                    event["shot"]["end_location"][0], event["shot"]["end_location"][1],
                    event["shot"]["key_pass_id"] if "key_pass_id" in event["shot"] else None,
                    event["shot"]["outcome"]["id"], 
                    event["shot"]["first_time"] if "first_time" in event["shot"] else None,
                    event["shot"]["type"]["id"],
                    event["shot"]["technique"]["id"], event["shot"]["body_part"]["id"]
                ))
                shot_id = cur.fetchone()[0]

                if "freeze_frame" in event["shot"]:

                    # Insert freeze frames
                    for frame in event["shot"]["freeze_frame"]:
                        cur.execute("""
                            INSERT INTO freeze_frames (
                                shot_id, location_x, location_y, player_id, position_id, teammate
                            )
                            VALUES (%s, %s, %s, %s, %s, %s);
                        """, (
                            shot_id, frame["location"][0], frame["location"][1],
                            frame["player"]["id"], frame["position"]["id"], frame["teammate"]
                        ))

        
        # Insert players
        players = set()
        for event in event_data:
            if "tactics" in event:
                for lineup in event["tactics"]["lineup"]:
                    players.add((lineup["player"]["id"], lineup["player"]["name"]))

        for player in players:
            cur.execute("""
                INSERT INTO players (player_id, player_name, player_nickname, country_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (player_id) DO NOTHING;
            """, (player[0], player[1], None, None))

        # Insert position types
        position_types = set()
        for event in event_data:
            if "tactics" in event:
                for lineup in event["tactics"]["lineup"]:
                    position_types.add((lineup["position"]["id"], lineup["position"]["name"]))

        for position_type in position_types:
            cur.execute("""
                INSERT INTO position_types (position_id, position_name)
                VALUES (%s, %s)
                ON CONFLICT (position_id) DO NOTHING;
            """, position_type)

        # Insert tactics and lineup data
        for event in event_data:
            if "tactics" in event:
                cur.execute("""
                    INSERT INTO tactics (event_id, formation)
                    VALUES (%s, %s)
                    RETURNING tactics_id;
                """, (event["id"], event["tactics"]["formation"]))
                tactics_id = cur.fetchone()[0]

                for lineup in event["tactics"]["lineup"]:
                    cur.execute("""
                        INSERT INTO tactics_lineup (tactics_id, player_id, position_id, jersey_number)
                        VALUES (%s, %s, %s, %s);
                    """, (
                        tactics_id,
                        lineup["player"]["id"],
                        lineup["position"]["id"],
                        lineup["jersey_number"]
                    ))



print("Data loaded successfully!")

# Commit the changes and close the connection
conn.commit()
conn.close()



# The Python script assumes that the JSON files are organized in the following directory structure:

# competitions.json
# matches/
#   season_id/
#       match_id.json
# lineups/
#       match_id.json
# events/
#       match_id.json





# Sample Queries:
# Q1: In the La Liga season of 2020/2021, sort the players from highest to lowest based on their average
# xG scores. Output both the player names and their average xG scores.
# SELECT 
#     p.player_name,
#     AVG(s.statsbomb_xg) AS avg_xg
# FROM 
#     shots s
#     JOIN events e ON s.event_id = e.event_id
#     JOIN matches m ON e.match_id = m.match_id
#     JOIN competitions c ON m.competition_id = c.competition_id
#     JOIN seasons se ON m.season_id = se.season_id
#     JOIN players p ON e.player_id = p.player_id
# WHERE 
#     c.competition_name = 'La Liga' 
#     AND se.season_name = '2020/2021'
# GROUP BY 
#     p.player_id, p.player_name
# ORDER BY 
#     avg_xg DESC;


# Q2:In the La Liga season of 2020/2021, find the players with the most shots. Sort them from highest to lowest. 
# Output both the player names and the number of shots. Consider only the players who made at least one shot (the lowest 
# number of shots should be 1, not 0).

# SELECT 
#     p.player_name,
#     COUNT(s.shot_id) AS shot_count
# FROM 
#     shots s
#     JOIN events e ON s.event_id = e.event_id
#     JOIN matches m ON e.match_id = m.match_id
#     JOIN competitions c ON m.competition_id = c.competition_id
#     JOIN seasons se ON m.season_id = se.season_id
#     JOIN players p ON e.player_id = p.player_id
# WHERE 
#     c.competition_name = 'La Liga' 
#     AND se.season_name = '2020/2021'
# GROUP BY 
#     p.player_id, p.player_name
# HAVING 
#     COUNT(s.shot_id) >= 1
# ORDER BY 
#     shot_count DESC;


# Q 3: In the La Liga seasons of 2020/2021, 2019/2020, and 2018/2019 combined, find the players with the most first-time shots. 
# Sort them from highest to lowest. Output the player names and the number of first time shots. Consider only the players who made 
# at least one shot (the lowest number of shots should be 1, not 0).

# SELECT 
#     p.player_name,
#     COUNT(s.shot_id) AS first_time_shot_count
# FROM 
#     shots s
#     JOIN events e ON s.event_id = e.event_id
#     JOIN matches m ON e.match_id = m.match_id
#     JOIN competitions c ON m.competition_id = c.competition_id
#     JOIN seasons se ON m.season_id = se.season_id
#     JOIN players p ON e.player_id = p.player_id
# WHERE 
#     c.competition_name = 'La Liga' 
#     AND se.season_name IN ('2020/2021', '2019/2020', '2018/2019')
#     AND s.first_time = true
# GROUP BY 
#     p.player_id, p.player_name
# HAVING 
#     COUNT(s.shot_id) >= 1
# ORDER BY 
#     first_time_shot_count DESC;



# Q4: In the La Liga season of 2020/2021, find the teams with the most passes made. Sort them from highest to lowest. Output the 
# team names and the number of passes. Consider the teams that make at least one pass (the lowest number of passes is 1, not 0).
# SELECT t.team_name, COUNT(p.pass_id) AS pass_count
# FROM passes p
# JOIN events e ON p.event_id = e.event_id
# JOIN matches m ON e.match_id = m.match_id
# JOIN competitions c ON m.competition_id = c.competition_id
# JOIN seasons s ON m.season_id = s.season_id
# JOIN teams t ON e.team_id = t.team_id
# WHERE c.competition_name = 'La Liga' AND s.season_name = '2020/2021'
# GROUP BY t.team_name
# HAVING COUNT(p.pass_id) >= 1
# ORDER BY pass_count DESC;


# Q5: In the Premier League season of 2003/2004, find the players who were the most intended recipients of passes. 
# Sort them from highest to lowest. Output the player names and the number of times they were the intended recipients of passes. 
# Consider the players who received at least one pass (the lowest number of times they were the intended recipients is 1, not 0).

# SELECT p.player_name, COUNT(pa.recipient_player_id) AS pass_count
# FROM passes pa
# JOIN events e ON pa.event_id = e.event_id
# JOIN matches m ON e.match_id = m.match_id
# JOIN competitions c ON m.competition_id = c.competition_id
# JOIN seasons s ON m.season_id = s.season_id
# JOIN players p ON pa.recipient_player_id = p.player_id
# WHERE c.competition_name = 'Premier League' AND s.season_name = '2003/2004'
# GROUP BY p.player_name
# HAVING COUNT(pa.recipient_player_id) >= 1
# ORDER BY pass_count DESC;


# Q6: In the Premier League season of 2003/2004, find the teams with the most shots made. 
# Sort them from highest to lowest. Output the team names and the number of shots. 
# Consider the teams that made at least one shot (the lowest number of shots is 1, not 0).

# SELECT t.team_name, COUNT(s.shot_id) AS shot_count
# FROM shots s
# JOIN events e ON s.event_id = e.event_id
# JOIN matches m ON e.match_id = m.match_id
# JOIN competitions c ON m.competition_id = c.competition_id
# JOIN seasons se ON m.season_id = se.season_id
# JOIN teams t ON e.team_id = t.team_id
# WHERE c.competition_name = 'Premier League' AND se.season_name = '2003/2004'
# GROUP BY t.team_name
# HAVING COUNT(s.shot_id) >= 1
# ORDER BY shot_count DESC; 


#  Q 7: In the La Liga season of 2020/2021, find the players who made the most through balls. Sort them from highest to lowest. 
#  Output the player names and the number of through balls. Consider the players who made at 
#  least one through ball pass (the lowest number of through balls is 1, not 0).

# SELECT p.player_name, COUNT(pa.pass_id) AS through_ball_count
# FROM passes pa
# JOIN events e ON pa.event_id = e.event_id
# JOIN matches m ON e.match_id = m.match_id
# JOIN competitions c ON m.competition_id = c.competition_id
# JOIN seasons s ON m.season_id = s.season_id
# JOIN players p ON e.player_id = p.player_id
# WHERE c.competition_name = 'La Liga' AND s.season_name = '2020/2021'
#   AND pa.through_ball = true
# GROUP BY p.player_name
# HAVING COUNT(pa.pass_id) >= 1
# ORDER BY through_ball_count DESC;



#  Q 8: In the La Liga season of 2020/2021, find the teams that made the most through balls. Sort them from highest to lowest. 
#  Output the team names and the number of through balls. Consider the teams with at least one through ball made in a match 
#  (the lowest total number of through balls is 1, not 0).

# SELECT t.team_name, COUNT(pa.pass_id) AS through_ball_count
# FROM passes pa
# JOIN events e ON pa.event_id = e.event_id
# JOIN matches m ON e.match_id = m.match_id
# JOIN competitions c ON m.competition_id = c.competition_id
# JOIN seasons s ON m.season_id = s.season_id
# JOIN teams t ON e.team_id = t.team_id
# WHERE c.competition_name = 'La Liga' AND s.season_name = '2020/2021'
#   AND pa.through_ball = true
# GROUP BY t.team_name
# HAVING COUNT(pa.pass_id) >= 1
# ORDER BY through_ball_count DESC;


# Q 9: In the La Liga seasons of 2020/2021, 2019/2020, and 2018/2019 combined, find the players that were the most successful 
# in completed dribbles. Sort them from highest to lowest. Output the player names and the number of successful completed dribbles. 
# Consider the players that made at least one successful dribble (the smallest number of successful dribbles is 1, not 0).

# SELECT p.player_name, COUNT(e.event_id) AS successful_dribbles
# FROM events e
# JOIN matches m ON e.match_id = m.match_id
# JOIN competitions c ON m.competition_id = c.competition_id
# JOIN seasons s ON m.season_id = s.season_id
# JOIN players p ON e.player_id = p.player_id
# WHERE c.competition_name = 'La Liga'
#   AND s.season_name IN ('2020/2021', '2019/2020', '2018/2019')
#   AND e.type_id = (SELECT type_id FROM event_types WHERE type_name = 'Dribble')
# GROUP BY p.player_name
# HAVING COUNT(e.event_id) >= 1
# ORDER BY successful_dribbles DESC;


# Q 10: In the La Liga season of 2020/2021, find the players that were least dribbled past. Sort them from lowest to highest. 
# Output the player names and the number of dribbles. Consider the players that were at least dribbled past once (the lowest number 
# of occurrences of dribbling past the player is 1, not 0).

# SELECT p.player_name, COUNT(e.event_id) AS dribbled_past_count
# FROM events e
# JOIN matches m ON e.match_id = m.match_id
# JOIN competitions c ON m.competition_id = c.competition_id
# JOIN seasons s ON m.season_id = s.season_id
# JOIN players p ON e.player_id = p.player_id
# WHERE c.competition_name = 'La Liga'
#   AND s.season_name = '2020/2021'
#   AND e.type_id = (SELECT type_id FROM event_types WHERE type_name = 'Dribbled Past')
# GROUP BY p.player_name
# HAVING COUNT(e.event_id) >= 1
# ORDER BY dribbled_past_count ASC;


# BONUS Queries
# -- Bonus Query 1:

# SELECT p.player_name, COUNT(s.shot_id) AS shots_in_top_corners
# FROM shots s
# JOIN events e ON s.event_id = e.event_id
# JOIN matches m ON e.match_id = m.match_id
# JOIN competitions c ON m.competition_id = c.competition_id
# JOIN seasons se ON m.season_id = se.season_id
# JOIN players p ON e.player_id = p.player_id
# WHERE c.competition_name = 'La Liga'
#   AND se.season_name IN ('2020/2021', '2019/2020', '2018/2019')
#   AND (
#     (s.end_location_x <= 120 / 3 AND s.end_location_y > 80 / 2) -- Top Left
#     OR
#     (s.end_location_x > 120 * 2 / 3 AND s.end_location_y > 80 / 2) -- Top Right
#   )
# GROUP BY p.player_name
# ORDER BY shots_in_top_corners DESC;


# -- Bonus Query 2:

# SELECT t.team_name, COUNT(pa.pass_id) AS successful_passes_into_box
# FROM passes pa
# JOIN events e ON pa.event_id = e.event_id
# JOIN matches m ON e.match_id = m.match_id
# JOIN competitions c ON m.competition_id = c.competition_id
# JOIN seasons s ON m.season_id = s.season_id
# JOIN teams t ON e.team_id = t.team_id
# WHERE c.competition_name = 'La Liga'
#   AND s.season_name = '2020/2021'
#   AND pa.recipient_player_id IS NOT NULL -- Assuming this indicates a successful pass
#   AND pa.end_location_x > (120 - 18) -- Assuming 120 is the total length and 18 is the penalty box length
#   AND pa.end_location_y BETWEEN (80 / 2 - 18 / 2) AND (80 / 2 + 18 / 2) -- Assuming 80 is the total width
# GROUP BY t.team_name
# ORDER BY successful_passes_into_box DESC;



