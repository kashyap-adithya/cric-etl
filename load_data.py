import os
import json
import psycopg2
from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_json_to_postgres(data_folder):
    """Reads JSON files from a folder and inserts them into PostgreSQL tables."""

    # Connect to PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Get all JSON files in the directory
    files = [f for f in os.listdir(data_folder) if f.endswith(".json")]

    if not files:
        print("No JSON files found in the folder.")
        return

    for file_name in files:
        file_path = os.path.join(data_folder, file_name)
        with open(file_path, "r") as f:
            data = json.load(f)

        # Extract match info
        match_info = data["info"]

        # Extract umpires from the correct location
        officials = match_info.get("officials", {})
        umpires = officials.get("umpires", [])

        # Handle cases where umpires list is missing or incomplete
        umpire_1 = umpires[0] if len(umpires) > 0 else "Unknown"
        umpire_2 = umpires[1] if len(umpires) > 1 else "Unknown"

        city = match_info.get("city", "Unknown") 

        outcome = match_info.get("outcome", {})
        winner = outcome.get("winner", "No Result")  # Use "No Result" if no winner
        win_by_wickets = outcome.get("by", {}).get("wickets", 0)  # Default to 0 if missing

        player_of_match = match_info.get("player_of_match", ["Unknown"])[0]


        # Insert match details into PostgreSQL
        cursor.execute(
            """
            INSERT INTO matches (season, match_date, city, venue, toss_winner, toss_decision, winner, win_by_wickets, player_of_match, umpire_1, umpire_2, team_1, team_2)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING match_id;
            """,
            (
                match_info["season"], match_info["dates"][0], city, match_info["venue"],
                match_info["toss"]["winner"], match_info["toss"]["decision"],
                winner, win_by_wickets,
                player_of_match, umpire_1, umpire_2, match_info["teams"][0], match_info["teams"][1]
            ),
        )
        match_id = cursor.fetchone()[0]

        # Insert teams
        for team in match_info["teams"]:
            cursor.execute("INSERT INTO teams (team_name) VALUES (%s) ON CONFLICT (team_name) DO NOTHING RETURNING team_id;", (team,))

        # Insert players (batter, bowler, non-striker)
        for inning_index, inning in enumerate(data["innings"], start=1):
            for over in inning["overs"]:
                for ball_index, delivery in enumerate(over["deliveries"], start=1):

                    # Extract players safely
                    batter = str(delivery.get("batter", "Unknown"))
                    bowler = str(delivery.get("bowler", "Unknown"))
                    non_striker = str(delivery.get("non_striker", "Unknown"))

                    # Extract runs safely
                    runs = delivery.get("runs", {})
                    runs_batter = int(runs.get("batter", 0))
                    runs_extras = int(runs.get("extras", 0))
                    runs_total = int(runs.get("total", 0))

                    # Extract dismissal details safely
                    dismissal_kind = None
                    fielder = None
                    if "wickets" in delivery and isinstance(delivery["wickets"], list) and delivery["wickets"]:
                        dismissal = delivery["wickets"][0]  # Get first wicket if it exists
                        dismissal_kind = str(dismissal.get("kind", "Unknown"))
                        fielders = dismissal.get("fielders", [])
                        if isinstance(fielders, list) and fielders:
                            first_fielder = fielders[0]  # Get first fielder dictionary
                            if isinstance(first_fielder, dict) and "name" in first_fielder:
                                fielder = first_fielder["name"] 

                    # Insert deliveries
                    cursor.execute(
                        """
                        INSERT INTO deliveries (match_id, inning, over_number, ball_number, batter, bowler, non_striker, 
                                                runs_batter, runs_extras, runs_total, wicket, dismissal_kind, fielder)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """,
                        (
                            match_id, inning_index, over["over"], ball_index, batter, bowler, non_striker,
                            runs_batter, runs_extras, runs_total, 
                            bool(delivery.get("wickets")), dismissal_kind, fielder
                        ),
                    )
                            
        conn.commit()
        print(f"Inserted data from {file_name}")

    cursor.close()
    conn.close()
    print("All JSON files loaded successfully.")