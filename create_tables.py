CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS matches (
    match_id SERIAL PRIMARY KEY,
    season VARCHAR(20),
    match_number INT,
    match_date DATE,
    city VARCHAR(50),
    venue VARCHAR(100),
    toss_winner VARCHAR(50),
    toss_decision VARCHAR(10),
    winner VARCHAR(50),
    win_by_runs INT,
    win_by_wickets INT,
    player_of_match VARCHAR(50),
    umpire_1 VARCHAR(50),
    umpire_2 VARCHAR(50),
    team_1 VARCHAR(50),
    team_2 VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS teams (
    team_id SERIAL PRIMARY KEY,
    team_name VARCHAR(50) UNIQUE
);

CREATE TABLE IF NOT EXISTS players (
    player_id SERIAL PRIMARY KEY,
    player_name VARCHAR(100) UNIQUE
);

CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id SERIAL PRIMARY KEY,
    match_id INT REFERENCES matches(match_id),
    inning INT,
    over_number INT,
    ball_number INT,
    batter VARCHAR(100),
    bowler VARCHAR(100),
    non_striker VARCHAR(100),
    runs_batter INT,
    runs_extras INT,
    runs_total INT,
    wicket BOOLEAN,
    dismissal_kind VARCHAR(50),
    fielder VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS match_features (
    match_id INT PRIMARY KEY,
    team_1 VARCHAR(50),
    team_2 VARCHAR(50),
    venue VARCHAR(100),
    toss_winner VARCHAR(50),
    toss_decision VARCHAR(10),
    team_1_batting_strength FLOAT,
    team_2_batting_strength FLOAT,
    team_1_bowling_strength FLOAT,
    team_2_bowling_strength FLOAT,
    team_1_recent_form FLOAT,
    team_2_recent_form FLOAT,
    team_1_h2h FLOAT,
    team_2_h2h FLOAT,
    team_1_venue_win_pct FLOAT,
    team_2_venue_win_pct FLOAT,
    team_1_toss_win_pct FLOAT,
    team_2_toss_win_pct FLOAT,
    actual_winner VARCHAR(50)
);
"""
