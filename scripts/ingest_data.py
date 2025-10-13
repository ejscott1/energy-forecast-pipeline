import os
import requests
import psycopg2
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from the .env file in the project root
load_dotenv()


def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        # Load the password securely from the environment
        db_password = os.getenv("DB_PASSWORD")
        if not db_password:
            raise ValueError("DB_PASSWORD not found in .env file!")

        conn = psycopg2.connect(
            dbname="energy_db",
            user="postgres",
            password=db_password,  # Use the variable here
            host="localhost",
            port="5432",
        )
        print("Database connection successful!")
        return conn
    except psycopg2.OperationalError as e:
        print(f"Could not connect to the database: {e}")
        return None


def fetch_eia_data(api_key):
    """Fetches hourly electricity demand data from the EIA API."""
    series_id = "EBA.US48-ALL.D.H"
    url = (
        f"https://api.eia.gov/v2/seriesid/{series_id}"
        f"?api_key={api_key}"
        "&facets[frequency][]=hourly"
        "&data[]=value"
        "&sort[0][column]=period"
        "&sort[0][direction]=desc"
        "&length=5000"
    )

    print("Fetching data from EIA API...")
    response = requests.get(url)

    if response.status_code == 200:
        print("API request successful!")
        data = response.json()
        return data["response"]["data"]
    else:
        print(f"Failed to fetch data from API. Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        return None


if __name__ == "__main__":
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        raise ValueError("EIA_API_KEY not found in .env file!")

    eia_data = fetch_eia_data(api_key)

    if eia_data:
        conn = get_db_connection()
        if conn:
            cur = conn.cursor()

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_demand (
                    timestamp TIMESTAMPTZ PRIMARY KEY,
                    demand_mwh INT
                );
            """
            )
            print("Table 'raw_demand' is ready.")

            insert_query = """
                INSERT INTO raw_demand (timestamp, demand_mwh)
                VALUES (%s, %s)
                 ON CONFLICT (timestamp) DO NOTHING;
            """
            data_to_insert = [
                (pd.to_datetime(row["period"]), row["value"]) for row in eia_data
            ]

            cur.executemany(insert_query, data_to_insert)
            conn.commit()

            print(f"Successfully inserted/updated {cur.rowcount} records.")

            cur.close()
            conn.close()
