import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from urllib.parse import quote_plus  # <-- 1. IMPORT THE NEW TOOL

# Load environment variables
load_dotenv()


def get_sql_engine():
    """Establishes a SQLAlchemy engine connection to the PostgreSQL database."""
    try:
        db_password = os.getenv("DB_PASSWORD")
        if not db_password:
            raise ValueError("DB_PASSWORD not found in .env file!")

        # <-- 2. ENCODE THE PASSWORD FOR SAFETY -->
        safe_password = quote_plus(db_password)

        # Create a connection "engine" using a database URL
        db_url = (
            f"postgresql://postgres:{safe_password}"  # <-- USE THE SAFE PASSWORD
            "@localhost:5432/energy_db"
        )

        engine = create_engine(db_url)
        print("Database connection engine created successfully!")
        return engine

    except Exception as e:
        print(f"Could not create database engine: {e}")
        return None


def load_raw_data(engine):
    """Loads all data from the raw_demand table into a Pandas DataFrame."""
    print("Loading data from 'raw_demand' table...")
    try:
        sql_query = "SELECT * FROM raw_demand;"
        df = pd.read_sql_query(sql_query, engine)

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index("timestamp", inplace=True)

        # We should sort our data by time, from oldest to newest
        df.sort_index(inplace=True)

        print(f"Successfully loaded {len(df)} records.")
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None


def transform_data(df):
    """Cleans and engineers features on the raw data DataFrame."""
    print("Transforming data...")

    # --- 1. Data Cleaning ---

    # We already sorted the index in the load step, which is great.
    # Let's check for any duplicate timestamps (should be handled by PRIMARY KEY, but good to check)
    df = df[~df.index.duplicated(keep="first")]

    # Check for missing values in 'demand_mwh'
    if df["demand_mwh"].isnull().sum() > 0:
        print(
            f"Found {df['demand_mwh'].isnull().sum()} missing values. Filling with 'forward fill' method."
        )
        # ffill (forward fill) is a good choice for time series,
        # it assumes the value was the same as the previous hour's.
        df["demand_mwh"] = df["demand_mwh"].ffill()

    # --- 2. Feature Engineering ---

    # Create time-based features
    # These give the model signals about time of day, week, etc.
    df["hour"] = df.index.hour
    df["day_of_week"] = df.index.dayofweek  # Monday=0, Sunday=6
    df["day_of_year"] = df.index.dayofyear
    df["month"] = df.index.month
    df["year"] = df.index.year

    # Create lag features
    # This is one of the most important features for forecasting.
    # What was the demand 24 hours ago? What was it 1 week ago?
    df["lag_demand_24h"] = df["demand_mwh"].shift(24)
    df["lag_demand_1_week"] = df["demand_mwh"].shift(24 * 7)

    # We'll also create a rolling average
    df["rolling_mean_24h"] = df["demand_mwh"].shift(1).rolling(window=24).mean()

    # After creating lag/rolling features, we will have NaNs at the start.
    # It's best to drop these rows as we can't train a model on them.
    df = df.dropna()

    print(f"Data transformed. New shape: {df.shape}")
    return df


def save_features_data(df, engine):
    """Saves the transformed DataFrame to a new table 'features_demand'."""
    print("Saving data to 'features_demand' table...")
    try:
        # Use df.to_sql to create/replace the table
        # 'if_exists='replace'' means it will drop the table if it already exists and create a new one.
        # This is useful during development.
        df.to_sql(
            "features_demand",
            engine,
            if_exists="replace",
            index=True,
            index_label="timestamp",
        )
        print(f"Successfully saved {len(df)} records to 'features_demand'.")
    except Exception as e:
        print(f"Error saving data: {e}")


if __name__ == "__main__":
    engine = get_sql_engine()
    if engine:
        # 1. Load the raw data
        raw_df = load_raw_data(engine)

        if raw_df is not None:
            print("\n--- Raw data loaded. Here's a preview: ---")
            print(raw_df.head())

            # 2. Transform the data
            transformed_df = transform_data(raw_df)

            if transformed_df is not None and not transformed_df.empty:
                print("\n--- Data transformed. Here's a preview: ---")
                print(transformed_df.head())

                # 3. Save the new features data
                save_features_data(transformed_df, engine)
            else:
                print("Data transformation failed or resulted in an empty DataFrame.")
        else:
            print("Data loading failed.")
