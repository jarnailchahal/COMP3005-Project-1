CREATE TABLE countries (
    country_id INT UNIQUE PRIMARY KEY,
    country_name VARCHAR(255)
);

CREATE TABLE seasons (
    season_id INT UNIQUE PRIMARY KEY,
    season_name VARCHAR(20)
);

CREATE TABLE referees (
    referee_id INT UNIQUE PRIMARY KEY,
    referee_name VARCHAR(255),
    country_id INT REFERENCES countries(country_id)
);

CREATE TABLE stadiums (
    stadium_id INT UNIQUE,
    stadium_name VARCHAR(255),
    country_id INT REFERENCES countries(country_id)
);


CREATE TABLE competitions (
    id SERIAL PRIMARY KEY,
    competition_id INT,
    country_name VARCHAR(255),
    competition_name VARCHAR(255),
    competition_gender VARCHAR(10),
    competition_youth BOOLEAN,
    competition_international BOOLEAN,
    season_id INT REFERENCES seasons(season_id)
);


CREATE TABLE teams (
    team_id INT UNIQUE,
    team_name VARCHAR(255),
    team_gender VARCHAR(10),
    team_group VARCHAR(255),
    country_id INT REFERENCES countries(country_id)
);

CREATE TABLE matches (
    match_id INT PRIMARY KEY,
    match_date DATE,
    kick_off TIME,
    competition_id INT,
    season_id INT REFERENCES seasons(season_id),
    home_team_id INT REFERENCES teams(team_id),
    away_team_id INT REFERENCES teams(team_id),
    home_score INT,
    away_score INT,
    match_week INT,
    competition_stage_id INT,
    stadium_id INT REFERENCES stadiums(stadium_id),
    referee_id INT REFERENCES referees(referee_id)
);


CREATE TABLE players (
    player_id INT PRIMARY KEY,
    player_name VARCHAR(255),
    player_nickname VARCHAR(255),
    country_id INT REFERENCES countries(country_id)
);

CREATE TABLE player_teams (
   id SERIAL PRIMARY KEY,
   player_id INT REFERENCES players(player_id),
   team_id INT REFERENCES teams(team_id),
   jersey_number INT
);


CREATE TABLE position_types (
    position_id INT PRIMARY KEY,
    position_name VARCHAR(255)
);

CREATE TABLE positions (
    id SERIAL PRIMARY KEY,
    player_id INT,
    match_id INT REFERENCES matches(match_id),
    position_id INT,
    "from" VARCHAR(255),
    "to" VARCHAR(255),
    from_period INT,
    to_period INT,
    start_reason VARCHAR(255),
    end_reason VARCHAR(255),
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (match_id) REFERENCES matches(match_id),
    FOREIGN KEY (position_id) REFERENCES position_types(position_id)
);

CREATE TABLE card_types (
    card_type_id SERIAL PRIMARY KEY,
    card_type_name VARCHAR(255) UNIQUE NOT NULL
);
CREATE TABLE reason_types (
    reason_type_id SERIAL PRIMARY KEY,
    reason_type_name VARCHAR(255) UNIQUE NOT NULL
);
CREATE TABLE cards (
    id SERIAL PRIMARY KEY,
    player_id INT,
    match_id INT,
    card_type_id INT,
    reason_type_id INT,
    time VARCHAR(10),
    period INT,
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (match_id) REFERENCES matches(match_id),
    FOREIGN KEY (card_type_id) REFERENCES card_types(card_type_id),
    FOREIGN KEY (reason_type_id) REFERENCES reason_types(reason_type_id)
);


CREATE TABLE event_types (
    type_id INT PRIMARY KEY,
    type_name VARCHAR(255)
);

CREATE TABLE play_patterns (
    pattern_id INT PRIMARY KEY,
    pattern_name VARCHAR(255)
);


CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(255) UNIQUE,
	match_id INT REFERENCES matches(match_id),
    event_index INT,
    period INT,
    "timestamp" VARCHAR(255),
    minute INT,
    second INT,
	type_id INT REFERENCES event_types(type_id),
    possession INT,
    possession_team_id INT REFERENCES teams(team_id),
    play_pattern_id INT REFERENCES play_patterns(pattern_id),
	team_id INT REFERENCES teams(team_id),
	player_id INT REFERENCES players(player_id),
    position_id INT REFERENCES position_types(position_id),
	location_x FLOAT,
	location_y FLOAT,	
	duration FLOAT,
    tactics_formation INT
);


CREATE TABLE related_events (
    event_id VARCHAR(255) REFERENCES events(event_id),
    related_event_id VARCHAR(255),
    PRIMARY KEY (event_id, related_event_id)
);

CREATE TABLE pass_heights (
	pass_height_id INT PRIMARY KEY,
    pass_height_name VARCHAR(255)
);

CREATE TABLE body_parts (
	body_part_id INT PRIMARY KEY,
    body_part_name VARCHAR(255)
);

CREATE TABLE passes (
    pass_id SERIAL PRIMARY KEY,
    event_id VARCHAR(255) REFERENCES events(event_id),
    recipient_player_id INT REFERENCES players(player_id),
    pass_length FLOAT,
    pass_angle FLOAT,
    pass_height_id INT REFERENCES pass_heights(pass_height_id),
    end_location_x FLOAT,
    end_location_y FLOAT,
    body_part_id INT REFERENCES body_parts(body_part_id),
	through_ball BOOLEAN,
    shot_assist BOOLEAN
);


CREATE TABLE shot_outcomes (
    outcome_id INT PRIMARY KEY,
    outcome_name VARCHAR(255)
);

CREATE TABLE shot_types (
    type_id INT PRIMARY KEY,
    type_name VARCHAR(255)
);

CREATE TABLE shot_techniques (
    technique_id INT PRIMARY KEY,	
    technique_name VARCHAR(255)
);

CREATE TABLE shots (
    shot_id SERIAL PRIMARY KEY,
    event_id VARCHAR(255) REFERENCES events(event_id),
    statsbomb_xg FLOAT,
    end_location_x FLOAT,
    end_location_y FLOAT,
	key_pass_id VARCHAR(255), -- 
    outcome_id INT REFERENCES shot_outcomes(outcome_id),
	first_time BOOLEAN,       -- 
    type_id INT REFERENCES shot_types(type_id),
    technique_id INT REFERENCES shot_techniques(technique_id),
    body_part_id INT REFERENCES body_parts(body_part_id)
);

CREATE TABLE freeze_frames (
    freeze_frame_id SERIAL PRIMARY KEY,
    shot_id INT REFERENCES shots(shot_id),
    location_x FLOAT,
    location_y FLOAT,
    player_id INT REFERENCES players(player_id),
    position_id INT REFERENCES position_types(position_id),
    teammate BOOLEAN
);


CREATE TABLE tactics (
    tactics_id SERIAL PRIMARY KEY,
    event_id VARCHAR(255) REFERENCES events(event_id),
    formation INT
);

CREATE TABLE tactics_lineup (
    lineup_id SERIAL PRIMARY KEY,
    tactics_id INT REFERENCES tactics(tactics_id),
    player_id INT REFERENCES players(player_id),
    position_id INT REFERENCES position_types(position_id),
    jersey_number INT
);

